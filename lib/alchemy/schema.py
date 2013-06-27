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


# PATENT ---------------------------


class Patent(Base):
    __tablename__ = "patent"
    uuid = Column(Integer, primary_key=True)
    type = Column(String(20))
    number = Column(String(20))
    country = Column(String(20))
    date = Column(Date)
    abstract = deferred(Column(Text))
    title = deferred(Column(Text))
    kind = Column(String(10))
    claims = Column(Integer)

    application = relationship("Application", uselist=False, backref="patent")
    assignees = relationship("Assignee", backref="patent")
    citations = relationship(
        "Citation",
        primaryjoin="Patent.uuid == Citation.patent_uuid",
        backref="patent")
    citedby = relationship(
        "Citation",
        primaryjoin="Patent.uuid == Citation.citation_uuid",
        backref="citation")
    classes = relationship("USPC", backref="patent")
    inventors = relationship("Inventor", backref="patent")
    othercitations = relationship("OtherCitation", backref="patent")

    kw = ["type", "number", "country", "date",
          "abstract", "title", "kind", "claims"]
    __table_args__ = (
        Index("pat_idx1", "type", "number", unique=True),
        Index("pat_idx2", "date"),
    )


class Application(Base):
    __tablename__ = "application"
    uuid = Column(Integer, primary_key=True)
    patent_uuid = Column(Integer, ForeignKey("patent.uuid"))
    type = Column(String(20))
    number = Column(String(20))
    country = Column(String(20))
    date = Column(Date)
    kw = ["type", "number", "country", "date"]
    __table_args__ = (
        Index("app_idx1", "type", "number", unique=True),
        Index("app_idx2", "date"),
    )
#Index('pat_idx3', Patent.date__grant)


# SUPPORT --------------------------


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
    kw = ["city", "state", "country", "longitude", "latitude"]
    __table_args__ = (
        Index("loc_idx1", "latitude", "longitude"),
    )

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


# OBJECTS --------------------------


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
    kw = ["sequence", "name_last", "name_first", "nationality"]
    __table_args__ = (
        ForeignKeyConstraint(
            [loc_city, loc_state, loc_country],
            [Location.city, Location.state, Location.country]
        ),
    )

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
    type = Column(String(10))
    organization = Column(String(256))
    name_first = Column(String(64))
    name_last = Column(String(64))
    loc_city = Column(String(128))
    loc_state = Column(String(10), index=True)
    loc_country = Column(String(10), index=True)
    sequence = Column(Integer, index=True)
    kw = ["sequence"]
    __table_args__ = (
        ForeignKeyConstraint(
            [loc_city, loc_state, loc_country],
            [Location.city, Location.state, Location.country]
        ),
    )

    def asg(self, *args):
        self.organization = args[0]
        self.type = args[1]

    def person(self, *args):
        self.name_last = args[0]
        self.name_first = args[1]


# REFERENCES -----------------------


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


# CLASSIFICATIONS ------------------


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
    text = Column(String(256))
    uspc = relationship("USPC", backref="mainclass")
    kw = ["id", "title", "text"]


class SubClass(Base):
    __tablename__ = "subclass"
    id = Column(String(20), primary_key=True)
    title = Column(String(256))
    text = Column(String(256))
    uspc = relationship("USPC", backref="subclass")
    kw = ["id", "title", "text"]
