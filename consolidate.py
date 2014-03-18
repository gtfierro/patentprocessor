#!/usr/bin/env python
"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Takes the existing database (as indicated by the alchemy configuration file) and creates
a dump CSV file with the appropriate columns as needed for the disambiguation:

  patent doc number, main class, sub class, inventor first name, inventor middle name, inventor last name,
  city, state, zipcode, country, assignee
"""
import codecs
from lib import alchemy
from lib.assignee_disambiguation import get_assignee_id
from lib.handlers.xml_util import normalize_utf8
from sqlalchemy.orm import joinedload, subqueryload
from sqlalchemy import extract
from datetime import datetime
import pandas as pd
import sys

#TODO: for ignore rows, use the uuid instead (leave blank if not ignore) and use that to link the ids together for integration
#TODO: include column in disambig input: inventor-id from previous run, or null

# create CSV file row using a dictionary. Use `ROW(dictionary)`
# isgrant: 1 if granted patent, 0 if application
# ignore: 1 if the record has a granted patent, 0 else
ROW = lambda x: u'{uuid}\t{isgrant}\t{ignore}\t{name_first}\t{name_middle}\t{name_last}\t{number}\t{mainclass}\t{subclass}\t{city}\t{state}\t{country}\t{assignee}\t{rawassignee}\n'.format(**x)

def main(year, doctype):
    # get patents as iterator to save memory
    # use subqueryload to get better performance by using less queries on the backend:
    # --> http://docs.sqlalchemy.org/en/latest/orm/tutorial.html#eager-loading
    session = alchemy.fetch_session(dbtype=doctype)
    schema = alchemy.schema.Patent
    if doctype == 'application':
        schema = alchemy.schema.App_Application
    if year:
        patents = (p for p in session.query(schema).filter(extract('year', schema.date) == year).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('classes')).yield_per(1))
    else:
        patents = (p for p in session.query(schema).options(subqueryload('rawinventors'), subqueryload('rawassignees'), subqueryload('classes')).yield_per(1))
    i = 0
    for patent in patents:
        i += 1
        if i % 100000 == 0:
          print i, datetime.now()
        try:
          # create common dict for this patent
          loc = patent.rawinventors[0].rawlocation.location
          mainclass = patent.classes[0].mainclass_id if patent.classes else ''
          subclass = patent.classes[0].subclass_id if patent.classes else ''
          row = {'number': patent.id,
                 'mainclass': mainclass,
                 'subclass': subclass,
                 'ignore': 0,
                 }
          if doctype == 'grant':
            row['isgrant'] = 1
          elif doctype == 'application':
            row['isgrant'] = 0
            if not patent.granted:
              row['ignore'] == 0
            elif int(patent.granted) == 1:
              row['ignore'] == 1
          row['assignee'] = get_assignee_id(patent.rawassignees[0]) if patent.rawassignees else ''
          row['assignee'] = row['assignee'].split('\t')[0]
          row['rawassignee'] = get_assignee_id(patent.rawassignees[0]) if patent.rawassignees else ''
          row['rawassignee'] = row['rawassignee'].split('\t')[0]
          # generate a row for each of the inventors on a patent
          for ri in patent.rawinventors:
              if not len(ri.name_first.strip()):
                  continue
              namedict = {'name_first': ri.name_first, 'uuid': ri.uuid}
              raw_name = ri.name_last.split(' ')
              # name_last is the last space-delimited word. Middle name is everything before that
              name_middle, name_last = ' '.join(raw_name[:-1]), raw_name[-1]
              namedict['name_middle'] = name_middle
              namedict['name_last'] = name_last
              loc = ri.rawlocation.location
              namedict['state'] = loc.state if loc else ''
              namedict['country'] = loc.country if loc else ''
              namedict['city'] = loc.city if loc else ''
              tmprow = row.copy()
              tmprow.update(namedict)
              newrow = normalize_utf8(ROW(tmprow))
              with codecs.open('disambiguator.csv', 'a', encoding='utf-8') as csv:
                  csv.write(newrow)
        except Exception as e:
          print e
          continue

def join(oldfile, newfile):
    new = pd.read_csv(newfile,delimiter='\t',header=None)
    old = pd.read_csv(oldfile,delimiter='\t',header=None)
    merged = pd.merge(new,old,on=0,how='left')
    merged.to_csv('disambiguator_{0}.tsv'.format(datetime.now().strftime('%B_%d')), index=False, header=None, sep='\t')

if __name__ == '__main__':
    if len(sys.argv) < 2:
      print "Provide path to previous disambiguation output"
      sys.exit(1)
    prev_output = sys.argv[1]
    for year in range(1975, datetime.today().year+1):
      print 'Running year',year,datetime.now(),'for grant'
      main(year, 'grant')
    for year in range(1975, datetime.today().year+1):
      print 'Running year',year,datetime.now(),'for application'
      main(year, 'application')

    # join files
    join(prev_output, 'disambiguator.csv')
