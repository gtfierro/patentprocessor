from sqlalchemy import Column, Date, Integer, Float
from sqlalchemy import ForeignKey, Index
from sqlalchemy import Unicode, UnicodeText
from sqlalchemy.orm import deferred, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base


# Extend the Base >>>>>>
Base = declarative_base()
cascade = "all, delete, delete-orphan"


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
    id = Column(Unicode(20), primary_key=True)
    type = Column(Unicode(20))
    number = Column(Unicode(20))
    country = Column(Unicode(20))
    date = Column(Date)
    abstract = deferred(Column(UnicodeText))
    title = deferred(Column(UnicodeText))
    kind = Column(Unicode(10))
    claims = Column(Integer)

    application = relationship("Application", uselist=False, backref="patent", cascade=cascade)
    classes = relationship("USPC", backref="patent", cascade=cascade)
    ipcrs = relationship("IPCR", backref="patent", cascade=cascade)

    assignees = relationship("Assignee", backref="patent", cascade=cascade)
    inventors = relationship("Inventor", backref="patent", cascade=cascade)
    lawyers = relationship("Lawyer", backref="patent", cascade=cascade)

    otherreferences = relationship("OtherReference", backref="patent", cascade=cascade)
    citations = relationship(
        "Citation",
        primaryjoin="Patent.id == Citation.patent_id",
        backref="patent", cascade=cascade)
    citedby = relationship(
        "Citation",
        primaryjoin="Patent.id == Citation.citation_id",
        backref="citation")
    usreldocs = relationship(
        "USRelDoc",
        primaryjoin="Patent.id == USRelDoc.patent_id",
        backref="patent", cascade=cascade)
    relpatents = relationship(
        "USRelDoc",
        primaryjoin="Patent.id == USRelDoc.rel_id",
        backref="relpatent")

    kw = ["id", "type", "number", "country", "date",
          "abstract", "title", "kind", "claims"]
    __table_args__ = (
        Index("pat_idx1", "type", "number", unique=True),
        Index("pat_idx2", "date"),
    )


class Application(Base):
    __tablename__ = "application"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    type = Column(Unicode(20))
    number = Column(Unicode(20))
    country = Column(Unicode(20))
    date = Column(Date)
    kw = ["type", "number", "country", "date"]
    __table_args__ = (
        Index("app_idx1", "type", "number"),
        Index("app_idx2", "date"),
    )
#Index('pat_idx3', Patent.date__grant)


# SUPPORT --------------------------


class Location(Base):
    #TODO: Anyway we can consolidate to 1 PrimaryKey
    __tablename__ = "location"
    id = Column(Unicode(256), primary_key=True)
    city = Column(Unicode(128))
    state = Column(Unicode(10), index=True)
    country = Column(Unicode(10), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    inventors = relationship("Inventor", backref="location")
    assignees = relationship("Assignee", backref="location")
    kw = ["city", "state", "country", "longitude", "latitude"]
    __table_args__ = (
        Index("loc_idx1", "latitude", "longitude"),
        Index("loc_idx2", "city", "state", "country"),
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


class Assignee(Base):
    __tablename__ = "assignee"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    type = Column(Unicode(10))
    organization = Column(Unicode(256))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    residence = Column(Unicode(10))
    nationality = Column(Unicode(10))
    location_id = Column(Unicode(256), ForeignKey("location.id"))
    sequence = Column(Integer, index=True)
    kw = ["sequence"]
    #__table_args__ = (
    #    ForeignKeyConstraint(
    #        [location_city, location_state, location_country],
    #        [Location.city, Location.state, Location.country]
    #    ),
    #)

    def asg(self, *args):
        self.organization = args[0]
        self.type = args[1]

    def person(self, *args):
        self.name_last = args[0]
        self.name_first = args[1]


class Inventor(Base):
    __tablename__ = "inventor"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    name_last = Column(Unicode(64))
    name_first = Column(Unicode(64))
    nationality = Column(Unicode(10))
    location_id = Column(Unicode(256), ForeignKey("location.id"))
    sequence = Column(Integer, index=True)
    kw = ["sequence", "name_last", "name_first", "nationality"]

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)


class Lawyer(Base):
    __tablename__ = "lawyer"
    uuid = Column(Unicode(36), primary_key=True)
    id = Column(Unicode(64), index=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    name_last = Column(Unicode(64))
    name_first = Column(Unicode(64))
    organization = Column(Unicode(64))
    country = Column(Unicode(10))
    sequence = Column(Integer, index=True)
    kw = ["sequence", "name_last", "name_first", "organization", "country"]

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)


# REFERENCES -----------------------


class Citation(Base):
    """
    Two types of citations?
    """
    __tablename__ = "citation"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    citation_id = Column(Unicode(20), ForeignKey("patent.id"))
    date = Column(Date, index=True)
    name = Column(Unicode(64))
    kind = Column(Unicode(10))
    number = Column(Unicode(20), index=True)
    country = Column(Unicode(10), index=True)
    category = Column(Unicode(20), index=True)
    sequence = Column(Integer, index=True)
    kw = ["sequence", "date", "number", "name",
          "kind", "country", "category"]


class OtherReference(Base):
    __tablename__ = "otherreference"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    text = deferred(Column(UnicodeText))
    sequence = Column(Integer, index=True)
    kw = ["sequence", "text"]


class USRelDoc(Base):
    __tablename__ = "usreldoc"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    rel_id = Column(Unicode(20), ForeignKey("patent.id"))
    doctype = Column(Unicode(64), index=True)
    status = Column(Unicode(20))
    date = Column(Date, index=True)
    number = Column(Unicode(20), index=True)
    kind = Column(Unicode(10))
    country = Column(Unicode(20), index=True)
    relationship = Column(Unicode(64))
    sequence = Column(Integer, index=True)


# CLASSIFICATIONS ------------------


class USPC(Base):
    __tablename__ = "uspc"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    mainclass_id = Column(Unicode(10), ForeignKey("mainclass.id"))
    subclass_id = Column(Unicode(10), ForeignKey("subclass.id"))
    sequence = Column(Integer, index=True)
    kw = ["uuid", "sequence"]


class IPCR(Base):
    __tablename__ = "ipcr"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    classification_level = Column(Unicode(20))
    section = Column(Unicode(20))
    subclass = Column(Unicode(20))
    main_group = Column(Unicode(20))
    subgroup = Column(Unicode(20))
    symbol_position = Column(Unicode(20))
    classification_value = Column(Unicode(20))
    classification_status = Column(Unicode(20))
    classification_data_source = Column(Unicode(20))
    action_date = Column(Date, index=True)
    ipc_version_indicator = Column(Date, index=True)
    sequence = Column(Integer, index=True)


class MainClass(Base):
    __tablename__ = "mainclass"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    text = Column(Unicode(256))
    uspc = relationship("USPC", backref="mainclass")
    kw = ["id", "title", "text"]


class SubClass(Base):
    __tablename__ = "subclass"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    text = Column(Unicode(256))
    uspc = relationship("USPC", backref="subclass")
    kw = ["id", "title", "text"]
