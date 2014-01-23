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
from lib.alchemy.schema import Inventor, RawInventor, patentinventor
from lib.handlers.xml_util import normalize_document_identifier
from collections import defaultdict
import cPickle as pickle
import linecache
from datetime import datetime
import pandas as pd
from collections import defaultdict, Counter
from lib.tasks import celery_commit_inserts, celery_commit_updates
from unidecode import unidecode
from datetime import datetime

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
    disambig_input = pd.read_csv(disambig_input_file,header=None,delimiter='\t',encoding='utf-8',skiprows=[1991872])
    disambig_output = pd.read_csv(disambig_output_file,header=None,delimiter='\t',encoding='utf-8',skiprows=[1991872])
    print 'finished loading csvs'
    merged = pd.merge(disambig_input, disambig_output, on=0)
    print 'finished merging'
    inventor_attributes = merged[[0,'1_y','1_x',2,3,4]] # rawinventor uuid, inventor id, first name, middle name, last name, patent_id
    inventor_attributes = inventor_attributes.dropna(subset=[0],how='all')
    inventor_attributes[2] = inventor_attributes[2].fillna('')
    inventor_attributes[3] = inventor_attributes[3].fillna('')
    inventor_attributes['1_x'] = inventor_attributes['1_x'].fillna('')
    print inventor_attributes
    rawinventors = defaultdict(list)
    inventor_inserts = []
    rawinventor_updates = []
    patentinventor_inserts = []
    for row in inventor_attributes.iterrows():
        uuid = row[1]['1_y']
        rawinventors[uuid].append(row[1])
        patentinventor_inserts.append({'inventor_id': uuid, 'patent_id': row[1][4]})
    print 'finished associating ids'
    i = 0
    for inventor_id in rawinventors.iterkeys():
        i += 1
        freq = defaultdict(Counter)
        param = {}
        rawuuids = []
        for raw in rawinventors[inventor_id]:
            rawuuids.append(raw[0])
            for k,v in raw.iteritems():
                freq[k][v] += 1
        param['id'] = inventor_id
        param['name_first'] = freq['1_x'].most_common(1)[0][0]
        param['name_last'] = ' '.join([unicode(freq[2].most_common(1)[0][0]), unicode(freq[3].most_common(1)[0][0])]).strip()
        param['name_last'] = unidecode(param['name_last'])
        param['nationality'] = ''
        assert set(param.keys()) == {'id','name_first','name_last','nationality'}
        inventor_inserts.append(param)
        for rawuuid in rawuuids:
            rawinventor_updates.append({'pk': rawuuid, 'update': param['id']})
        if i % 100000 == 0:
            print i, datetime.now(), rawuuids[0]
    print 'finished voting'

    t1 = celery_commit_inserts.delay(inventor_inserts, Inventor.__table__, is_mysql(), 20000)
    t2 = celery_commit_inserts.delay(patentinventor_inserts, patentinventor, is_mysql(), 20000)
    t3 = celery_commit_updates.delay('inventor_id', rawinventor_updates, RawInventor.__table__, is_mysql(), 20000)
    t1.get()
    t2.get()
    t3.get()

def main():
    if len(sys.argv) <= 2:
        print 'USAGE: python integrate.py <disambig input file> <disambig output file>'
        sys.exit()
    dis_in = sys.argv[1]
    dis_out = sys.argv[2]
    integrate(dis_in, dis_out)

if __name__ == '__main__':
    main()
