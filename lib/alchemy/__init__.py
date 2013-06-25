import os

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import backref, relation, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


import ConfigParser
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


Session = sessionmaker(bind=engine)
Base = declarative_base()
session = Session()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    fullname = Column(String(50))
    password = Column(String(50))

    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password


Base.metadata.create_all(engine)
user = User('ed', 'ed', 'ed')
session.add(user)
session.commit()
