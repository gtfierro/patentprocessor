import os
import re
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
from collections import defaultdict
import schema
from match import *


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


def fetch_session(db=None, dbtype='grant'):
    """
    Read from config.ini file and load appropriate database

    @db: string describing database, e.g. "sqlite" or "mysql"
    @dbtype: string indicating if we are fetching the session for
             the grant database or the application database
    """
    config = get_config()
    echo = config.get('global').get('echo')
    if not db:
        db = config.get('global').get('database')
    if db[:6] == "sqlite":
        sqlite_db_path = os.path.join(
            config.get(db).get('path'),
            config.get(db).get('{0}-database'.format(dbtype)))
        engine = create_engine('sqlite:///{0}'.format(sqlite_db_path), echo=echo)
    else:
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
            config.get(db).get('user'),
            config.get(db).get('password'),
            config.get(db).get('host'),
            config.get(db).get('{0}-database'.format(dbtype)), echo=echo))
    schema.GrantBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def add_grant(obj, override=True, temp=False):
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
    (patent_exists, ), = session.query(exists().where(schema.Patent.number == obj.patent))
    #pat_query = session.query(Patent).filter(Patent.number == obj.patent)
    #if pat_query.count():
    if patent_exists:
        if override:
            pat_query = session.query(schema.Patent).filter(schema.Patent.number == obj.patent)
            session.delete(pat_query.one())
        else:
            return
    if len(obj.pat["number"]) < 3:
        return

    pat = schema.Patent(**obj.pat)
    pat.application = schema.Application(**obj.app)
    # lots of abstracts seem to be missing. why?
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
    add_claims(obj, pat)


def add_asg(obj, pat):
    for asg, loc in obj.assignee_list:
        asg = schema.RawAssignee(**asg)
        loc = schema.RawLocation(**loc)
        session.merge(loc)
        asg.rawlocation = loc
        pat.rawassignees.append(asg)


def add_inv(obj, pat):
    for inv, loc in obj.inventor_list:
        inv = schema.RawInventor(**inv)
        loc = schema.RawLocation(**loc)
        session.merge(loc)
        inv.rawlocation = loc
        pat.rawinventors.append(inv)


def add_law(obj, pat):
    for law in obj.lawyer_list:
        law = schema.RawLawyer(**law)
        pat.rawlawyers.append(law)


def add_usreldoc(obj, pat):
    for usr in obj.us_relation_list:
        usr["rel_id"] = usr["number"]
        usr = schema.USRelDoc(**usr)
        pat.usreldocs.append(usr)


def add_classes(obj, pat):
    for uspc, mc, sc in obj.us_classifications:
        uspc = schema.USPC(**uspc)
        mc = schema.MainClass(**mc)
        sc = schema.SubClass(**sc)
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)


def add_ipcr(obj, pat):
    for ipc in obj.ipcr_classifications:
        ipc = schema.IPCR(**ipc)
        pat.ipcrs.append(ipc)


def add_citations(obj, pat):
    cits, refs = obj.citation_list
    for cit in cits:
        if cit['country'] == 'US':
            # granted patent doc number
            if re.match(r'^[A-Z]*\d+$', cit['number']):
                cit['citation_id'] = cit['number']
                cit = schema.USPatentCitation(**cit)
                pat.uspatentcitations.append(cit)
            # if not above, it's probably an application
            else:
                cit['application_id'] = cit['number']
                cit = schema.USApplicationCitation(**cit)
                pat.usapplicationcitations.append(cit)
        # if not US, then foreign citation
        else:
            cit = schema.ForeignCitation(**cit)
            pat.foreigncitations.append(cit)
    for ref in refs:
        ref = schema.OtherReference(**ref)
        pat.otherreferences.append(ref)

def add_claims(obj, pat):
    claims = obj.claims
    for claim in claims:
        clm = schema.Claim(**claim)
        pat.claims.append(clm)


def commit():
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)


session = fetch_session()
