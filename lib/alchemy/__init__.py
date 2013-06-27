import os
import ConfigParser

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from schema import *


def fetch_engine():
    """
    Read from config.ini file and load appropriate database
    """
    echo = False
    config = ConfigParser.ConfigParser()
    config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))
    if config.get('global', 'database') == "sqlite":
        engine = create_engine('sqlite:///{0}'.format(config.get('sqlite', 'database')), echo=echo)
    else:
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=latin1'.format(
            config.get('mysql', 'user'),
            config.get('mysql', 'password'),
            config.get('mysql', 'host'),
            config.get('mysql', 'database')), echo=echo)
    return engine


def add(obj):
    """
    PatentGrant Object converting to tables via SQLAlchemy
    Necessary to convert dates to datetime because of SQLite (OK on MySQL)
    """
    #add patent
    # lots of abstracts seem to be missing. why?
    pat = Patent(**obj.pat)
    pat.application = Application(**obj.app)

    #+asg
    for asg, loc in obj.assignee_list():
        asg = Assignee(**asg)
        loc = Location(**loc)
        session.merge(loc)
        asg.location = loc
        pat.assignees.append(asg)

    #+inv
    for inv, loc in obj.inventor_list():
        inv = Inventor(**inv)
        loc = Location(**loc)
        session.merge(loc)
        inv.location = loc
        pat.inventors.append(inv)

    #+cit
    for cit in obj.citation_list():
        cit = Citation(**cit)
        pat.citations.append(cit)

    #+othercit
    for ref in obj.citation_list(category="other"):
        ref = OtherReference(**ref)
        pat.otherreferences.append(ref)

    #+law
    for law in obj.lawyer_list():
        law = Lawyer(**law)
        pat.lawyers.append(law)

    # ----------------------------------------

    #+classes (TODO for later)
    for i, cls in enumerate(obj.classes):
        uspc = USPC(i)
        mc = MainClass(cls[0])
        sc = SubClass("/".join(cls))
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)

    #add usreldocs
    # us reldocs looks a bit problematic. ruh roh
    #
    # -- SAMPLE --
    # 6 ['CONTINUATION-IN-PART', 1, u'12082601', u'US', '', u'20080412', u'PENDING', u'20080412', u'PENDING', u'20080412', u'PENDING']
    # 7 ['CONTINUATION-IN-PART', -1, u'12082601', u'US', '', '', '', '', '', '', '']
    # 8 ['CONTINUATION-IN-PART', 1, u'12079179', u'US', '', u'20080325', u'PENDING', u'20080325', u'PENDING', u'20080325', u'PENDING', u'20080325', u'PENDING']
    # 9 ['CONTINUATION-IN-PART', -1, u'12079179', u'US', '', '', '', '', '', '', '', '', '']
    # 10 ['CONTINUATION-IN-PART', 1, u'11593271', u'US', '', u'20061106', '', u'20061106', '', u'20061106', '', u'20061106', '', u'20061106', '', u'7511589', u'US', '', '', '', '', '', '', '', '', '', '', '']
    # 11 ['CONTINUATION-IN-PART', 1, u'11593271', u'US', '', u'20061106', '', u'20061106', '', u'20061106', '', u'20061106', '', u'20061106', '', u'7511589', u'US', '', '', '', '', '', '', '', '', '', '', '']
    # 12 ['CONTINUATION-IN-PART', -1, u'11593271', u'US', '', '', '', '', '', '', '', '', '', '', '']
    # 13 ['CONTINUATION-IN-PART', 1, u'11500125', u'US', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'7525392', u'US', '', '', '', '', '', '', '', '', '', '', '', '', '']
    # 14 ['CONTINUATION-IN-PART', 1, u'11500125', u'US', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'20060805', '', u'7525392', u'US', '', '', '', '', '', '', '', '', '', '', '', '', '']
    # 0 ['CONTINUATION', -1, u'12964855', u'US', '']
    #for i, usr in enumerate(obj.rel_list):
    #    print i, usr

    session.merge(pat)
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)


engine = fetch_engine()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
