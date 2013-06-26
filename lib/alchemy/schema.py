from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import Unicode, UnicodeText, ForeignKey, Index, Text
from sqlalchemy.orm import backref, deferred, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base


# Extend the Base >>>>>>
Base = declarative_base()


def init(self, *args, **kwargs):
    for i, arg in enumerate(args):
        self.__dict__[self.kw[i]] = arg
    for k, v in kwargs.iteritems():
        self.__dict__[k] = v
Base.__init__ = init
# <<<<<<


class Patent(Base):
    __tablename__ = "patent"
    uuid = Column(Integer, primary_key=True)
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
    classes = relationship("USPC", backref="patent")

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
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    mainclass_id = Column(String(10), ForeignKey("mainclass.id"))
    #subclass_id = Column(Integer, ForeignKey("subclass.id"))
    sequence = Column(Integer, index=True)
    kw = ["sequence"]


class MainClass(Base):
    __tablename__ = "mainclass"
    id = Column(String(20), primary_key=True, unique=True)
    title = Column(String(256))
    description = Column(String(256))
    uspc = relationship("USPC", backref="mainclass")
    kw = ["id", "title", "description"]


class SubClass(Base):
    __tablename__ = "subclass"
    id = Column(String(20), primary_key=True, unique=True)
    title = Column(String(256))
    description = Column(String(256))
    #uspc = relationship("USPC", backref="subclass")
    kw = ["id", "title", "description"]
