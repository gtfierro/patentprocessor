#!/usr/bin/env python
"""
Takes the existing database (as indicated by the alchemy configuration file) and creates
a dump CSV file with the appropriate columns as needed for the disambiguation:

  patent doc number, main class, sub class, inventor first name, inventor middle name, inventor last name,
  city, state, zipcode, country, assignee
"""

from lib import alchemy
from lib.assignee_disambiguation import get_assignee_id
from lib.handlers.xml_util import normalize_utf8

# get patents as iterator to save memory
patents = (p for p in alchemy.session.query(alchemy.Patent).yield_per(1))

# create CSV file row using a dictionary. Use `ROW(dictionary)`
ROW = lambda x: u'{number}, {mainclass}, {subclass}, {name_first}, {name_middle}, {name_last},\
{state}, {zipcode}, {latitude}, {longitude}, {country}, {assignee}\n'.format(**x)

insert_rows = []

for patent in patents:
    # create common dict for this patent
    loc = patent.rawinventors[0].rawlocation.location
    row = {'number': patent.number,
           'mainclass': patent.classes[0].mainclass_id,
           'subclass': patent.classes[0].subclass_id,
           'state': loc.state,
           'zipcode': '',
           'latitude': loc.latitude,
           'longitude': loc.longitude,
           'country': loc.country,
           'city': loc.city,
           }
    row['assignee'] = get_assignee_id(patent.assignees[0]) if patent.assignees else ''
    # generate a row for each of the inventors on a patent
    for ri in patent.rawinventors:
        namedict = {'name_first': ri.name_first}
        raw_name = ri.name_last.split(' ')
        # name_last is the last space-delimited word. Middle name is everything before that
        name_middle, name_last = ' '.join(raw_name[:-1]), raw_name[-1]
        namedict['name_middle'] = name_middle
        namedict['name_last'] = name_last
        tmprow = row.copy()
        tmprow.update(namedict)
        newrow = normalize_utf8(ROW(tmprow))
        insert_rows.append(newrow)

with open('disambiguator.csv', 'wb') as csv:
    for row in insert_rows:
        csv.write(row)
