from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import Unicode, UnicodeText, ForeignKey, Index, Text
from sqlalchemy.orm import deferred, relationship
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
    inventors = relationship("Inventor", backref="patent")
    assignees = relationship("Assignee", backref="patent")

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


class Inventor(Base):
    __tablename__ = "inventor"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    name_last = Column(String(64))
    name_first = Column(String(64))
    addr_city = Column(String(128))
    addr_state = Column(String(10), index=True)
    addr_country = Column(String(10), index=True)
    addr_latitude = Column(Float)
    addr_longitude = Column(Float)
    sequence = Column(Integer, index=True)

    __table_args__ = (
        Index("inv_idx1", "addr_city", "addr_state", "addr_country"),
        Index("inv_idx2", "addr_latitude", "addr_longitude"),
    )
    kw = ["sequence", "name_last", "name_first",
          "addr_city", "addr_state", "addr_country", "nationality",
          "addr_latitude", "addr_longitude"]

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    @hybrid_property
    def address(self):
        addy = []
        if self.addr_city:
            addy.append(self.addr_city)
        if self.addr_state:
            addy.append(self.addr_state)
        if self.addr_country:
            addy.append(self.addr_country)
        return ", ".join(addy)


class Assignee(Base):
    __tablename__ = "assignee"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    asg_type = Column(String(10))
    asg_name = Column(String(256))
    name_first = Column(String(64))
    name_last = Column(String(64))
    addr_city = Column(String(128))
    addr_state = Column(String(10), index=True)
    addr_country = Column(String(10), index=True)
    addr_latitude = Column(Float)
    addr_longitude = Column(Float)
    sequence = Column(Integer, index=True)

    __table_args__ = (
        Index("asg_idx1", "addr_city", "addr_state", "addr_country"),
        Index("asg_idx2", "addr_latitude", "addr_longitude"),
    )
    kw = ["sequence", "addr_city", "addr_state", "addr_country"]

    def asg(self, *args):
        self.asg_name = args[0]
        self.asg_type = args[1]

    def person(self, *args):
        self.name_last = args[0]
        self.name_first = args[1]

    @hybrid_property
    def address(self):
        addy = []
        if self.addr_city:
            addy.append(self.addr_city)
        if self.addr_state:
            addy.append(self.addr_state)
        if self.addr_country:
            addy.append(self.addr_country)
        return ", ".join(addy)


class USPC(Base):
    __tablename__ = "uspc"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    mainclass_id = Column(String(10), ForeignKey("mainclass.id"))
    subclass_id = Column(String(10), ForeignKey("subclass.id"))
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
