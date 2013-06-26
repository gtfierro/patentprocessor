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
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
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
    date_grant = datetime.strptime(obj.date_grant, '%Y%m%d')
    date_app = datetime.strptime(obj.date_app, '%Y%m%d')
    pat = Patent(obj.pat_type, obj.patent, obj.country, date_grant,
                 obj.code_app, obj.patent_app, obj.country_app, date_app,
                 obj.abstract, obj.invention_title, obj.kind, obj.clm_num)

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
        pat.inventors.append(Inventor(
            i, inv[0], inv[1], inv[3], inv[4], inv[5], inv[8]))

    #add assignee
    # is there some sort of assignee number?
    # some of the patents don't have assignees. why?
    # for example: 8089324
    #
    # -- SAMPLE --
    # [0, u'Huizhou Light Engine Ltd.', u'03', '', u'Huizhou, Guangdong', '', u'CN', '', '', '']
    # [0, u'Koninklijke Philips Electronics N.V.', u'03', '', u'Eindhoven', '', u'NL', '', '', '']
    # [0, u'Kueberit Profile Systems GmbH &amp; Co. KG', u'03', '', u'Luedenscheid', '', u'DE', '', '', '']
    for i, asg in enumerate(obj.asg_list):
        pa = Assignee(i, asg[4], asg[5], asg[6])
        if asg[0] == 0:
            pa.asg(asg[1], asg[2])
        else:
            pa.asg(asg[1], asg[2])
        pat.assignees.append(pa)


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
