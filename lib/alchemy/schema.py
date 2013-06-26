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
    grant_type = Column(String(20))
    grant_num = Column(String(20))
    grant_country = Column(String(20))
    date_grant = Column(Date)
    app_type = Column(String(20))
    app_num = Column(String(20))
    app_country = Column(String(20))
    date_app = Column(Date)
    abstract = deferred(Column(Text))
    title = deferred(Column(Text))
    kind = Column(String(10))
    claims = Column(Integer)
    classes = relationship("USPC", backref="patent")

    __table_args__ = (
        Index("pat_idx1", "grant_type", "grant_num", unique=True),
        Index("pat_idx2", "app_type", "app_num", unique=True),
        Index("pat_idx3", "date_grant"),
        Index("pat_idx4", "date_app"),
    )
    kw = ["grant_type", "grant_num", "grant_country", "date_grant",
          "app_type", "app_num", "app_country", "date_app",
          "abstract", "title", "kind", "claims"]
#Index('pat_idx3', Patent.date__grant)


class USPC(Base):
    __tablename__ = "uspc"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    mainclass_id = Column(String(10), ForeignKey("mainclass.id"))
    subclass_id = Column(Integer, ForeignKey("subclass.id"))
    sequence = Column(Integer, index=True)
    kw = ["sequence"]


class MainClass(Base):
    __tablename__ = "mainclass"
    id = Column(String(20), primary_key=True)
    title = Column(String(256))
    description = Column(String(256))
    uspc = relationship("USPC", backref="mainclass")
    kw = ["id", "title", "description"]


class SubClass(Base):
    __tablename__ = "subclass"
    id = Column(String(20), primary_key=True)
    title = Column(String(256))
    description = Column(String(256))
    uspc = relationship("USPC", backref="subclass")
    kw = ["id", "title", "description"]
