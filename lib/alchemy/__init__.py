import os
import ConfigParser

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, validates
from schema import *


def fetch_engine():
    """
    Read from config.ini file and load appropriate database
    """
    echo = True
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

    for i, cls in enumerate(obj.classes):
        uspc = USPC(i)
        mc = MainClass(cls[0])
        sc = SubClass("/".join(cls))
        session.merge(mc)
        session.merge(sc)
        uspc.mainclass = mc
        uspc.subclass = sc
        pat.classes.append(uspc)
    session.merge(pat)
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)


def commit():
    print "hi"
    session.commit()


engine = fetch_engine()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
