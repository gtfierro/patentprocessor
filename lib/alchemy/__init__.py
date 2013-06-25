import os

from sqlalchemy import create_engine
from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import Unicode, UnicodeText, ForeignKey, Text
from sqlalchemy.orm import backref, deferred, relationship
from sqlalchemy.orm import sessionmaker, validates
from sqlalchemy.ext.hybrid import hybrid_property
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


class Patent(Base):
    __tablename__ = "patent"
    id = Column(Integer, primary_key=True)
    grant__type = Column(String(20))
    grant__num = Column(String(20))
    date__grant = Column(Date)
    app__type = Column(String(20))
    app__num = Column(String(20))
    date__app = Column(Date)
    abstract = deferred(Column(Text))
    title = deferred(Column(Text))
    kind = Column(String(10))
    claims = Column(Integer)

    kw = ["grant__type", "grant__num", "date__grant",
          "app__type", "app__num", "date__app",
          "abstract", "title", "kind", "claims"]

    def __init__(self, *args, **kwargs):
        for i, arg in enumerate(args):
            self.__dict__[self.kw[i]] = arg
        for k, v in kwargs.iteritems():
            self.__dict__[k] = v


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50))
    fullname = Column(String(50))
    password = Column(String(50))

    def __init__(self, name, fullname, password):
        self.name = name
        self.fullname = fullname
        self.password = password

    @hybrid_property
    def fullname2(self):
        return self.name + "---" + self.fullname

    @validates('password')
    def validate_email(self, key, address):
        assert "@" in address
        return address

#   deferred column loading
#   deferred(Column(Text))
#   read @ deferred data (for title, abstract and least)

Base.metadata.create_all(engine)
user = User('ed', 'mcedon', 'e@d')
pat = Patent("D", "21321", "2005-12-12")
pat.kind = "F"
session.add(pat)
session.add(Patent(date__grant="20040502", date__app="2004-2-2"))

print user.fullname2
session.add(user)
session.commit()
