import os

from sqlalchemy import create_engine
from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import Unicode, UnicodeText, ForeignKey, Index, Text
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

"""
Here I am modifying the Declarative Base
"""
def init(self, *args, **kwargs):
    for i, arg in enumerate(args):
        self.__dict__[self.kw[i]] = arg
    for k, v in kwargs.iteritems():
        self.__dict__[k] = v
Base.__init__ = init


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

    __table_args__ = (
        Index("pat_idx1", "grant__type", "grant__num", unique=True),
        Index("pat_idx2", "app__type", "app__num", unique=True),
        Index("pat_idx3", "date__grant"),
        Index("pat_idx4", "date__app"),
    )

    kw = ["grant__type", "grant__num", "date__grant",
          "app__type", "app__num", "date__app",
          "abstract", "title", "kind", "claims"]
#Index('pat_idx3', Patent.date__grant)


class USPC(Base):
    __tablename__ = "uspc"
    id = Column(String(20), primary_key=True, unique=True)
    class_id = Column(String(10), ForeignKey("class.id"))
    sequence = Column(Integer)
    title = Column(String(256))
    description = Column(String(256))


class Class(Base):
    __tablename__ = "class"
    id = Column(String(20), primary_key=True, unique=True)
    title = Column(String(256))
    description = Column(String(256))


"""
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
"""

Base.metadata.create_all(engine)
"""
user = User('ed', 'mcedon', 'e@d')
print user.fullname2
session.add(user)
"""
pat = Patent("D", "21321")
pat.kind = "F"
session.add(pat)
session.add(Patent(grant__num="22321"))
#session.save_or_update(Patent(grant__num="22321", grant__date="20040205"))

session.commit()
