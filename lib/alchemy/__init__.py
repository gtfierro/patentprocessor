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
    # abstracts seem to be missing. why?
    date_grant = datetime.strptime(obj.date_grant, '%Y%m%d')
    date_app = datetime.strptime(obj.date_app, '%Y%m%d')
    pat = Patent(obj.patent, obj.pat_type, obj.patent, obj.country, date_grant,
                 obj.abstract, obj.invention_title, obj.kind, obj.clm_num)
    pat.application = Application(obj.code_app, obj.patent_app,
                                  obj.country_app, date_app)

    for asg, loc in obj.assignee_list():
        asg = Assignee(**asg)
        loc = Location(**loc)
        session.merge(loc)
        asg.location = loc
        pat.assignees.append(asg)

    for cit in obj.citation_list():
        cit = Citation(**cit)
        pat.citations.append(cit)

    for ref in obj.citation_list(category="other"):
        ref = OtherReference(**ref)
        pat.otherreferences.append(ref)


    #add classes
    for i, cls in enumerate(obj.classes):
        uspc = USPC(i)
        mc = MainClass(cls[0])
        sc = SubClass("/".join(cls))
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)

    #add inventors
    # what's up with middle name and suffix? what is this omitted?
    #
    # -- SAMPLE --
    # 0 [u'Minami', u'Masaki', '', u'Wakayama', '', u'JP', '', u'omitted', u'JP']
    # 1 [u'Minamide', u'Hiroshi', '', u'Wakayama', '', u'JP', '', u'omitted', u'JP']
    # 2 [u'Nishitani', u'Hirokazu', '', u'Wakayama', '', u'JP', '', u'omitted', u'JP']
    for i, inv in enumerate(obj.inv_list):
        iv = Inventor(i, inv[0], inv[1], inv[8])
        lc = Location(inv[3], inv[4], inv[5])
        session.merge(lc)
        iv.location = lc
        pat.inventors.append(iv)



    """
    for i, asg in enumerate(obj.asg_list):
        pa = Assignee(i)
        lc = Location(inv[3], inv[4], inv[5])
        session.merge(lc)
        if asg[0] == 0:
            pa.asg(asg[1], asg[2])
        else:
            pa.asg(asg[1], asg[2])
        pa.location = lc
        pat.assignees.append(pa)
    """

    #add lawyer
    # does this have city, state, country?
    #
    # -- SAMPLE --
    # 0 ['', '', u'unknown', u'Wenderoth, Lind & Ponack, L.L.P.']
    # 0 [u'Tran', u'Bao', u'unknown', '']
    # 1 [u'Kolodka', u'Joseph', u'unknown', '']
    # 0 [u'C. Basch', u'Duane', u'unknown', '']
    for i, law in enumerate(obj.law_list):
        lc = Lawyer(i, law[0], law[1], law[3], law[2])
        pat.lawyers.append(lc)

    #add citation
    # other citation
    # is there a way we can tell the doc number as a US patent?
    # I see lots of different patent types and numbers
    # - citation date is just YYYY-MM-00
    # Seperate both citation and othercitation
    #
    # -- SAMPLE --
    # 0 [u'cited by examiner', u'US', u'4672559', u'19870600', u'A', u'Jansson et al.', '']
    # 1 [u'cited by examiner', u'US', u'5999189', u'19991200', u'A', u'Kajiya et al.', '']
    # 2 [u'cited by examiner', u'US', u'6052492', u'20000400', u'A', u'Bruckhaus', '']
    # 3 [u'cited by examiner', u'US', u'6282327', u'20010800', u'B1', u'Betrisey et al.', '']
    #15 [u'cited by other', '', '', '', '', '', u'Pagoulatos et al.: \u201cInteractive 3-D Registration of Ultrasound and Magnetic Resonance Images Based on a Magnetic Positio

    #h = 0
    #for i, cit in enumerate(obj.cit_list):
    #    if cit[3]:
    #        cit[3] = cit[3][:7] + "1"
    #        date = datetime.strptime(cit[3], '%Y%m%d')
    #        pc = Citation(i, date, cit[2], cit[5], cit[4], cit[1], cit[0])
    #        pat.citations.append(pc)
    #    else:
    #        pc = OtherReference(h, cit[6])
    #        pat.otherreferences.append(pc)
    #        h += 1

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

    #print obj.inventor_list()
    print ""

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
