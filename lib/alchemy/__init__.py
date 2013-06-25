import os
import schema
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, validates


def set_engine():
    """
    Read from config.ini file and load appropriate database
    """
    config = ConfigParser.ConfigParser()
    config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))
    if config.get('global', 'database') == "sqlite":
        engine = create_engine('sqlite:///{0}'.format(config.get('sqlite', 'database')))
    else:
        engine = create_engine('mysql+mysqldb://{0}:{1}@{2}/{3}?charset=utf8'.format(
            config.get('mysql', 'user'),
            config.get('mysql', 'password'),
            config.get('mysql', 'host'),
            config.get('mysql', 'database')))
    return engine


Session = sessionmaker(bind=engine)
session = Session()
Base = schema.feth_base()
Base.metadata.create_all(engine)


pat = Patent("D", "21321")
pat.kind = "F"
session.add(pat)
session.add(Patent(grant__num="22321"))

session.commit()
