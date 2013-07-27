import sqlalchemy as alchemy
import sqlalchemy.orm as orm
import sqlalchemy.ext.declarative as declarative

base = declarative.declarative_base()

class UniqueRawGoogle(base):
    __tablename__ = 'unique_raw_google'
    id = alchemy.Column("rowid", alchemy.Integer,primary_key=True)
    input_address = alchemy.Column(alchemy.String)
    output_address = alchemy.Column(alchemy.String)
    hierarchy = alchemy.Column(alchemy.String)
    latitude = alchemy.Column("lat", alchemy.REAL)
    longitude = alchemy.Column("long", alchemy.REAL)
    
    def __init__(self, input_address, output_address, hierarchy, latitude, longitude):
        self.input_address=input_address
        self.output_address=output_address
        self.hierarchy=hierarchy
        self.latitude=latitude
        self.longitude=longitude
    
    def __repr__(self):
        return "<RawGoogle('%s','%s','%s','%s','%s')>" % (self.input_address, self.output_address, self.hierarchy, self.latitude, self.longitude)
    
class AllRawGoogle(base):
    __tablename__ = 'all_raw_google'
    id = alchemy.Column("rowid", alchemy.Integer,primary_key=True)
    input_address = alchemy.Column(alchemy.String)
    output_address = alchemy.Column(alchemy.String)
    hierarchy = alchemy.Column(alchemy.String)
    latitude = alchemy.Column("lat", alchemy.REAL)
    longitude = alchemy.Column("long", alchemy.REAL)
    
    def __init__(self, input_address, output_address, hierarchy, latitude, longitude):
        self.input_address=input_address
        self.output_address=output_address
        self.hierarchy=hierarchy
        self.latitude=latitude
        self.longitude=longitude
    
    def __repr__(self):
        return "<RawGoogle('%s','%s','%s','%s','%s')>" % (self.input_address, self.output_address, self.hierarchy, self.latitude, self.longitude)

def init(self):
    self.raw_google_dbpath = 'data/raw_google.sqlite3'
    self.raw_google_engine = alchemy.create_engine('sqlite:///%s' % self.raw_google_dbpath)
    self.raw_google_session_class = orm.sessionmaker(bind=self.raw_google_engine)
    self.raw_google_session = self.raw_google_session_class()
    
def main(self):
    print self.raw_google_dbpath
    