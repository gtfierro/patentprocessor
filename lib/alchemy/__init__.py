import os
import re
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from collections import defaultdict
from collections import Counter
from schema import *


def get_config(localfile="config.ini", default_file=True):
    """
    This grabs a configuration file and converts it into
    a dictionary.

    The default filename is called config.ini
    First we load the global file, then we load a local file
    """
    if default_file:
        openfile = "{0}/config.ini".format(os.path.dirname(os.path.realpath(__file__)))
    else:
        openfile = localfile
    config = defaultdict(dict)
    if os.path.isfile(openfile):
        cfg = ConfigParser.ConfigParser()
        cfg.read(openfile)
        for s in cfg.sections():
            for k, v in cfg.items(s):
                dec = re.compile('\d+(\.\d+)?')
                if v in ("True", "False") or v.isdigit() or dec.match(v):
                    v = eval(v)
                config[s][k] = v

    # this enables us to load a local file
    if default_file:
        newconfig = get_config(localfile, default_file=False)
        for section in newconfig:
            for item in newconfig[section]:
                config[section][item] = newconfig[section][item]

    return config


def fetch_session(db=None):
    """
    Read from config.ini file and load appropriate database
    """
    config = get_config()
    echo = config.get('global').get('echo')
    if not db:
        db = config.get('global').get('database')
    if db[:6] == "sqlite":
        sqlite_db_path = os.path.join(
            config.get(db).get('path'),
            config.get(db).get('database'))
        #Delete any existing database if we should refresh
        #Actually doesn't work properly - it calls itself on clean.py as well! Needs more work
        #if(config.get('sqlite').get('refresh')):
        #    try:
        #        os.remove(sqlite_db_path)
        #    except:
        #        pass
        engine = create_engine('sqlite:///{0}'.format(sqlite_db_path), echo=echo)
    else:
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
            config.get(db).get('user'),
            config.get(db).get('password'),
            config.get(db).get('host'),
            config.get(db).get('database')), echo=echo)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def match(objects, session, default={}, keepexisting=False):
    """
    Pass in several objects and make them equal

    Args:
        objects: A list of RawObjects like RawAssignee
          also supports CleanObjects like Assignee
        keepexisting: Keep the default keyword
        default: Fields to default the clean variable with

        Default key priority:
        auto > keepexisting > default
    """
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None

    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            clean = obj.__clean__
        else:
            clean = obj
            obj = None

        if clean and clean not in clean_objects:
            clean_objects.append(clean)
            if len(clean.__raw__) > clean_cnt:
                clean_cnt = len(clean.__raw__)
                clean_main = clean
            # figures out the most frequent items
            if not keepexisting:
                for k in clean.__related__.summarize:
                    freq[k] += Counter(dict(clean.__rawgroup__(session, k)))
        elif obj and obj not in raw_objects:
            raw_objects.append(obj)

    exist_param = {}
    if clean_main:
        exist_param = clean_main.summarize

    if keepexisting:
        param = exist_param
    else:
        param = exist_param
        for obj in raw_objects:
            for k, v in obj.summarize.iteritems():
                if k not in default:
                    freq[k][v] += 1
            if "id" not in exist_param:
                if "id" not in param:
                    param["id"] = obj.uuid
                param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]
    param.update(default)

    # remove all clean objects
    if len(clean_objects) > 1:
        for obj in clean_objects:
            clean_main.relink(session, obj)
        session.commit()  # commit necessary

        # for some reason you need to delete this after the initial commit
        for obj in clean_objects:
            if obj != clean_main:
                session.delete(obj)

    if clean_main:
        relobj = clean_main
        relobj.update(**param)
    else:
        cleanObj = objects[0].__related__
        cleanCnt = session.query(cleanObj).filter(cleanObj.id == param["id"])
        if cleanCnt.count() > 0:
            relobj = cleanCnt.first()
            relobj.update(**param)
        else:
            relobj = objects[0].__related__(**param)
    # associate the data into the related object

    for obj in raw_objects:
        relobj.relink(session, obj)

    session.merge(relobj)
    session.commit()


def unmatch(objects, session):
    """
    Separate our dataset
    # TODO. THIS NEEDS TO BE FIGURED OUT
    # Unlinking doesn't seem to be working
    # properly if a LOCATION is added
    """
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            obj.unlink(session)
        else:
            session.delete(obj)
            session.commit()


def add(obj, override=True, temp=False):
    """
    PatentGrant Object converting to tables via SQLAlchemy
    Necessary to convert dates to datetime because of SQLite (OK on MySQL)

    Case Sensitivity and Table Reflection
    MySQL has inconsistent support for case-sensitive identifier names,
    basing support on specific details of the underlying operating system.
    However, it has been observed that no matter what case sensitivity
    behavior is present, the names of tables in foreign key declarations
    are always received from the database as all-lower case, making it
    impossible to accurately reflect a schema where inter-related tables
    use mixed-case identifier names.

    Therefore it is strongly advised that table names be declared as all
    lower case both within SQLAlchemy as well as on the MySQL database
    itself, especially if database reflection features are to be used.
    """

    # if a patent exists, remove it so we can replace it
    pat_query = session.query(Patent).filter(Patent.number == obj.patent)
    if pat_query.count():
        if override:
            session.delete(pat_query.one())
        else:
            return
    if len(obj.pat["number"]) < 3:
        return

    #add
    # lots of abstracts seem to be missing. why?

    pat = Patent(**obj.pat)
    pat.application = Application(**obj.app)
    
    add_all_fields(obj, pat)

    session.merge(pat)
    
def add_all_fields(obj, pat):
    add_asg(obj, pat)
    add_inv(obj, pat)
    add_law(obj, pat)
    add_usreldoc(obj, pat)
    add_classes(obj, pat)
    add_ipcr(obj, pat)
    add_citations(obj, pat)
    
def add_asg(obj, pat):
    for asg, loc in obj.assignee_list:
        asg = RawAssignee(**asg)
        loc = RawLocation(**loc)
        session.merge(loc)
        asg.rawlocation = loc
        pat.rawassignees.append(asg)

def add_inv(obj, pat):
    for inv, loc in obj.inventor_list:
        inv = RawInventor(**inv)
        loc = RawLocation(**loc)
        session.merge(loc)
        inv.rawlocation = loc
        pat.rawinventors.append(inv)

def add_law(obj, pat):
    for law in obj.lawyer_list:
        law = RawLawyer(**law)
        pat.rawlawyers.append(law)
        
def add_usreldoc(obj, pat):
    for usr in obj.us_relation_list:
        usr = USRelDoc(**usr)
        pat.usreldocs.append(usr)
        
def add_classes(obj, pat):
    for uspc, mc, sc in obj.us_classifications:
        uspc = USPC(**uspc)
        mc = MainClass(**mc)
        sc = SubClass(**sc)
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)
        
def add_ipcr(obj, pat):
    for ipc in obj.ipcr_classifications:
        ipc = IPCR(**ipc)
        pat.ipcrs.append(ipc)
        
def add_citations(obj, pat):
    cits, refs = obj.citation_list
    for cit in cits:
        if cit['country'] == 'US':
            # granted patent doc number
            if re.match(r'^[A-Z]*\d+$', cit['number']):
                cit = USPatentCitation(**cit)
                pat.uspatentcitations.append(cit)
            # if not above, it's probably an application
            else:
                cit = USApplicationCitation(**cit)
                pat.usapplicationcitations.append(cit)
        # if not US, then foreign citation
        else:
            cit = ForeignCitation(**cit)
            pat.foreigncitations.append(cit)
    for ref in refs:
        ref = OtherReference(**ref)
        pat.otherreferences.append(ref)

def commit():
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)


session = fetch_session()
