from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy import ForeignKeyConstraint, ForeignKey, Index, Text
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
    othercitations = relationship("OtherCitation", backref="patent")
    citations = relationship(
        "Citation",
        primaryjoin="Patent.uuid == Citation.patent_uuid",
        backref="patent")
    citedby = relationship(
        "Citation",
        primaryjoin="Patent.uuid == Citation.citation_uuid",
        backref="citation")

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


class Location(Base):
    #TODO: Anyway we can consolidate to 1 PrimaryKey
    __tablename__ = "location"
    city = Column(String(128), primary_key=True)
    state = Column(String(10), index=True, primary_key=True)
    country = Column(String(10), index=True, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    inventors = relationship("Inventor", backref="location")
    assignees = relationship("Assignee", backref="location")

    __table_args__ = (
        Index("loc_idx1", "latitude", "longitude"),
    )
    kw = ["city", "state", "country", "longitude", "latitude"]

    @hybrid_property
    def address(self):
        addy = []
        if self.city:
            addy.append(self.city)
        if self.state:
            addy.append(self.state)
        if self.country:
            addy.append(self.country)
        return ", ".join(addy)


class Inventor(Base):
    __tablename__ = "inventor"
    uuid = Column(Integer, primary_key=True)
    #location_uuid = Column(Integer, ForeignKey("location.uuid"))
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    name_last = Column(String(64))
    name_first = Column(String(64))
    loc_city = Column(String(128))
    loc_state = Column(String(10), index=True)
    loc_country = Column(String(10), index=True)
    sequence = Column(Integer, index=True)

    __table_args__ = (
        ForeignKeyConstraint(
            [loc_city, loc_state, loc_country],
            [Location.city, Location.state, Location.country]
        ),
    )
    kw = ["sequence", "name_last", "name_first", "nationality"]

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)


class Assignee(Base):
    __tablename__ = "assignee"
    uuid = Column(Integer, primary_key=True)
    #location_uuid = Column(Integer, ForeignKey("location.uuid"))
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    asg_type = Column(String(10))
    asg_name = Column(String(256))
    name_first = Column(String(64))
    name_last = Column(String(64))
    loc_city = Column(String(128))
    loc_state = Column(String(10), index=True)
    loc_country = Column(String(10), index=True)
    sequence = Column(Integer, index=True)

    __table_args__ = (
        ForeignKeyConstraint(
            [loc_city, loc_state, loc_country],
            [Location.city, Location.state, Location.country]
        ),
    )
    kw = ["sequence"]

    def asg(self, *args):
        self.asg_name = args[0]
        self.asg_type = args[1]

    def person(self, *args):
        self.name_last = args[0]
        self.name_first = args[1]


class Citation(Base):
    """
    Two types of citations?
    """
    __tablename__ = "citation"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    citation_uuid = Column(Integer, ForeignKey("patent.uuid"))
    date = Column(Date, index=True)
    number = Column(String(20), index=True)
    name = Column(String(64))
    kind = Column(String(10))
    country = Column(String(10), index=True)
    category = Column(String(20), index=True)
    sequence = Column(Integer, index=True)
    kw = ["sequence", "date", "number", "name",
          "kind", "country", "category"]


class OtherCitation(Base):
    __tablename__ = "othercitation"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    text = Column(Text)
    sequence = Column(Integer, index=True)
    kw = ["sequence", "text"]


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
