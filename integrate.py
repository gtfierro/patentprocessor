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
from lib.alchemy import is_mysql
from lib.alchemy.schema import Inventor, RawInventor
from lib.handlers.xml_util import normalize_document_identifier
from collections import defaultdict
import cPickle as pickle
import linecache
from datetime import datetime
import pandas as pd
from collections import defaultdict, Counter
from lib.tasks import celery_commit_inserts, celery_commit_updates
from unidecode import unidecode

def integrate(disambig_input_file, disambig_output_file):
    """
    We have two files: the input to the disambiguator:
        uuid, first name, middle name, last name, patent, mainclass, subclass, city, state, country, rawassignee, disambiguated assignee
    And the output of the disambiguator:
        uuid, unique inventor id

    The files will line up line by line, so we can easily get the collection of raw
    records that map to a single disambiguated record (D_REC).  For each of the raw records
    for a given disambiguated id (D_ID), we want to vote the most frequent values for
    each of the columns, and use those to populate the D_REC.


    just have to populate the fields of the disambiguated inventor object:
        inventor id, first name, last name, nationality (?)
    """
    disambig_input = pd.read_csv(disambig_input_file,header=None,delimiter='\t',encoding='utf-8')
    disambig_output = pd.read_csv(disambig_output_file,header=None,delimiter='\t',encoding='utf-8')
    merged = pd.merge(disambig_input, disambig_output, on=0)
    inventor_attributes = merged[['1_y','1_x',2,3]] # inventor id, first name, middle name, last name
    rawinventors = defaultdict(list)
    inventor_inserts = []
    rawinventor_updates = []
    for row in inventor_attributes.iterrows():
        uuid = row[1]['1_y']
        rawinventors[uuid].append(row[1])
    for inventor_id in rawinventors.iterkeys():
        freq = defaultdict(Counter)
        param = {}
        for raw in rawinventors[inventor_id]:
            for k,v in raw.iteritems():
                freq[k][v] += 1
        param['id'] = freq['1_y'].most_common(1)[0][0]
        param['name_first'] = freq['1_x'].most_common(1)[0][0]
        param['name_last'] = unicode(freq[2].most_common(1)[0][0]) + ' ' + unicode(freq[3].most_common(1)[0][0])
        param['name_last'] = unidecode(param['name_last'])
        param['nationality'] = ''
        inventor_inserts.append(param)
        for row in merged[merged['1_y'] == param['id']].iterrows():
            rawinventor_updates.append({'pk': row[1][0], 'update': row[1]['1_y']})

    t1 = celery_commit_inserts.delay(inventor_inserts, Inventor.__table__, is_mysql(), 20000)
    t2 = celery_commit_updates.delay('inventor_id', rawinventor_updates, RawInventor.__table__, is_mysql(), 20000)
    t1.get()
    t2.get()

def main():
    if len(sys.argv) <= 2:
        print 'USAGE: python integrate.py <disambig input file> <disambig output file>'
        sys.exit()
    dis_in = sys.argv[1]
    dis_out = sys.argv[2]
    integrate(dis_in, dis_out)

if __name__ == '__main__':
    main()
