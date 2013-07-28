import sqlalchemy
import sqlalchemy.orm as orm
import sqlalchemy.ext.declarative as declarative
import geoalchemy_util
import itertools

import alchemy

base = declarative.declarative_base()

class RawGoogle(base):
    __tablename__ = 'raw_google'
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
        return "<RawGoogle('%s','%s','%s','%s','%s')>" % (self.input_address, self.output_address, self.hierarchy, self.latitude, self.longitude)

raw_google_dbpath = 'lib/raw_google.sqlite3'
raw_google_engine = sqlalchemy.create_engine('sqlite:///%s' % raw_google_dbpath)
raw_google_session_class = orm.sessionmaker(bind=raw_google_engine)
raw_google_session = raw_google_session_class()
    
def main():
    #Get all of the raw locations from the XML parsing
    parsed_raw_locations = alchemy.session.query(alchemy.RawLocation)
    grouped_locations=[]
    for instance in parsed_raw_locations:
        #Convert the location into a string that matches the Google format
        parsed_raw_location = geoalchemy_util.concatenate_location(instance.city, instance.state, instance.country)
        cleaned_location = geoalchemy_util.clean_raw_location(parsed_raw_location)
        #Find the location from the raw_google database that matches this input
        matching_location = raw_google_session.query(RawGoogle).filter_by(input_address=cleaned_location).first()
        alchemy.match(instance)
        if(matching_location):
            #Group by latitude and longitude for now. Round to
            #encourage a little more overlap
            sig_figs = 3
            try:
                grouping_id = "%s|%s" % (round(matching_location.latitude, sig_figs),
                                     round(matching_location.longitude, sig_figs))
            except:
                print matching_location.latitude, matching_location.longitude
            grouped_locations.append({"raw_location":instance, 
                                      "matching_location":matching_location,
                                      "grouping_id":grouping_id})
    print "grouped_locations created"
    #We now have a list of all locations in the file, along with their
    #matching locations and the id used to group them
    #Sort the list by the grouping_id
    keyfunc = lambda x:x[2]
    grouped_locations.sort(key=keyfunc)
    #Group by the grouping_id
    for key, grouping in itertools.groupby(grouped_locations, keyfunc):
        #match_group is the list of RawLocation objects which we call match on
        #We need to get only the RawLocation objects back from the dict
        match_group = []
        latitude = grouping[0]["latitude"]
        longitude = grouping[0]["longitude"]
        for grouped_location in grouping:
            match_group.append(grouped_location["raw_location"])
        alchemy.match(match_group, {"latitude":latitude, "longitude":longitude})
        print "match called"