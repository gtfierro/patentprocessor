import os
import ConfigParser

from schema import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, validates


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
    PatentGrant Object converting to MySQL tables via SQLAlchemy
    """
    pat = Patent(obj.pat_type, obj.patent)
    for i, cls in enumerate(obj.classes):
        uspc = USPC(i)
        mc = session.query(MainClass).filter_by(id=cls[0]).one()
        print mc
        #uspc.mainclass = MainClass(cls[0])
        #ucpc.subclass = SubClass("/".join(cls))
        pat.classes.append(uspc)

    session.add(pat)
    try:
        session.commit()
    except Exception, e:
        session.rollback()
        print str(e)    


engine = fetch_engine()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
