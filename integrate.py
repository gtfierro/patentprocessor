#!/usr/bin/env python
"""
Takes in a CSV file that represents the output of the disambiguation engine:
  Patent Number, Firstname, Lastname, Unique_Inventor_ID
Groups by Unique_Inventor_ID and then inserts them into the Inventor table using
lib.alchemy.match
"""

import sys
import lib.alchemy as alchemy
from lib.util.csv_reader import read_file
from lib.handlers.xml_util import normalize_document_identifier
from collections import defaultdict
import cPickle as pickle
import linecache
from datetime import datetime


def integrate(filename, disambiginput):
    blocks = defaultdict(list)
    print 'Gathering blocks'
    for index, line in enumerate(read_file(filename)):
        if index % 100000 == 0:
          print index, str(datetime.now())
        unique_inventor_id = line[0]
        oldline = linecache.getline(disambiginput, index+1).split('\t')
        patent_number, name_first, name_last = oldline[0], oldline[2], oldline[3]
        patent_number = normalize_document_identifier(patent_number)
        rawinventors = alchemy.session.query(alchemy.schema.RawInventor).filter_by(
                                patent_id = patent_number,
                                name_first = name_first,
                                name_last = name_last).all()
        blocks[unique_inventor_id].extend(rawinventors)
    pickle.dump(blocks, open('integrate.db', 'wb'))
    print 'Starting commits'
    i = 0
    for block in blocks.itervalues():
        i += 1
        if i % 10000 == 0:
          print i
          alchemy.match(block, alchemy.session, commit=True)
          print str(datetime.now())
        else:
          alchemy.match(block, alchemy.session, commit=False)

    alchemy.match(block, alchemy.session)

def main():
    if len(sys.argv) <= 1:
        print 'USAGE: python integrate.py <path-to-csv-file>'
        sys.exit()
    filename = sys.argv[1]
    disambiginput = sys.argv[2]
    integrate(filename, disambiginput)

if __name__ == '__main__':
    main()
