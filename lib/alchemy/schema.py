from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import Unicode, UnicodeText, ForeignKey, Index, Text
from sqlalchemy.orm import backref, deferred, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


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
