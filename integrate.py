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


def integrate(filename):
    blocks = defaultdict(list)
    for line in read_file(filename):
        patent_number, name_first, name_last, unique_inventor_id = line
        patent_number = normalize_document_identifier(patent_number)
        rawinventors = alchemy.session.query(alchemy.RawInventor).filter_by(
                                patent_id = patent_number,
                                name_first = name_first,
                                name_last = name_last).all()
        blocks[unique_inventor_id].extend(rawinventors)
    pickle.dump(blocks, open('integrate.db', 'wb'))
    for block in blocks.itervalues():
        alchemy.match(block)

def main():
    if len(sys.argv) <= 1:
        print 'USAGE: python integrate.py <path-to-csv-file>'
        sys.exit()
    filename = sys.argv[1]
    integrate(filename)

if __name__ == '__main__':
    main()
