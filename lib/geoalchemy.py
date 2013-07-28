import sqlalchemy
import sqlalchemy.orm as orm
import sqlalchemy.ext.declarative as declarative
import geoalchemy_util

import alchemy

base = declarative.declarative_base()

class UniqueRawGoogle(base):
    __tablename__ = 'unique_raw_google'
    id = sqlalchemy.Column("rowid", sqlalchemy.Integer,primary_key=True)
    input_address = sqlalchemy.Column(sqlalchemy.String)
    output_address = sqlalchemy.Column(sqlalchemy.String)
    hierarchy = sqlalchemy.Column(sqlalchemy.String)
    latitude = sqlalchemy.Column(sqlalchemy.REAL)
    longitude = sqlalchemy.Column(sqlalchemy.REAL)
    
    def __init__(self, input_address, output_address, hierarchy, latitude, longitude):
        self.input_address=input_address
        self.output_address=output_address
        self.hierarchy=hierarchy
        self.latitude=latitude
        self.longitude=longitude
    
    def __repr__(self):
        return "<UniqueRawGoogle('%s','%s','%s','%s','%s')>" % (self.input_address, self.output_address, self.hierarchy, self.latitude, self.longitude)
    
class AllRawGoogle(base):
    __tablename__ = 'all_raw_google'
    id = sqlalchemy.Column("rowid", sqlalchemy.Integer,primary_key=True)
    input_address = sqlalchemy.Column(sqlalchemy.String)
    output_address = sqlalchemy.Column(sqlalchemy.String)
    hierarchy = sqlalchemy.Column(sqlalchemy.String)
    latitude = sqlalchemy.Column(sqlalchemy.REAL)
    longitude = sqlalchemy.Column(sqlalchemy.REAL)
    
    def __init__(self, input_address, output_address, hierarchy, latitude, longitude):
        self.input_address=input_address
        self.output_address=output_address
        self.hierarchy=hierarchy
        self.latitude=latitude
        self.longitude=longitude
    
    def __repr__(self):
        return "<AllRawGoogle('%s','%s','%s','%s','%s')>" % (self.input_address, self.output_address, self.hierarchy, self.latitude, self.longitude)

raw_google_dbpath = 'lib/raw_google.sqlite3'
raw_google_engine = sqlalchemy.create_engine('sqlite:///%s' % raw_google_dbpath)
raw_google_session_class = orm.sessionmaker(bind=raw_google_engine)
raw_google_session = raw_google_session_class()
    
def main():
    pass