import os
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schema import *


def fetch_session(db=None):
    """
    Read from config.ini file and load appropriate database
    """
    echo = False
    config = ConfigParser.ConfigParser()
    config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))
    if not db:
        db = config.get('global', 'database')
    if db[:6] == "sqlite":
        engine = create_engine('sqlite:///{0}'.format(config.get(db, 'database')), echo=echo)
    else:
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
            config.get(db, 'user'),
            config.get(db, 'password'),
            config.get(db, 'host'),
            config.get(db, 'database')), echo=echo)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


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

    #+asg
    for asg, loc in obj.assignee_list():
        asg = RawAssignee(**asg)
        loc = RawLocation(**loc)
        session.merge(loc)
        asg.rawlocation = loc
        pat.rawassignees.append(asg)

    #+inv
    for inv, loc in obj.inventor_list():
        inv = RawInventor(**inv)
        loc = RawLocation(**loc)
        session.merge(loc)
        inv.rawlocation = loc
        pat.rawinventors.append(inv)

    #+law
    for law in obj.lawyer_list():
        law = RawLawyer(**law)
        pat.rawlawyers.append(law)

    #+usreldoc
    for usr in obj.us_relation_list():
        usr = USRelDoc(**usr)
        pat.usreldocs.append(usr)

    #+classes
    for uspc, mc, sc in obj.us_classifications():
        uspc = USPC(**uspc)
        mc = MainClass(**mc)
        sc = SubClass(**sc)
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)

    #+ipcr
    for ipc in obj.ipcr_classifications():
        ipc = IPCR(**ipc)
        pat.ipcrs.append(ipc)

    # citations are huge. this dumps them to
    # a temporary database which we can use for later
    if temp:
        cits, refs = obj.citation_list()
        for cit in cits:
            cit["patent_id"] = obj.pat["number"]
            cit = TempCitation(**cit)
            session.add(cit)
        for ref in refs:
            ref["patent_id"] = obj.pat["number"]
            ref = TempOtherReference(**ref)
            session.add(ref)
    else:
        cits, refs = obj.citation_list()
        for cit in cits:
            cit = Citation(**cit)
            pat.citations.append(cit)
        for ref in refs:
            ref = OtherReference(**ref)
            pat.otherreferences.append(ref)

    session.merge(pat)


def commit():
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)


session = fetch_session()
