import re
import os
import sys
import parse
import time
import itertools
import datetime
from IPython.parallel import Client
import requests
import zipfile
import cStringIO as StringIO
from BeautifulSoup import BeautifulSoup as bs

sys.path.append('lib')
from patSQL import *
from config_parser import get_config_options

# table bookkeeping for the parse script

assignee_table = AssigneeSQL()
citation_table = CitationSQL()
class_table = ClassSQL()
inventor_table = InventorSQL()
patent_table = PatentSQL()
patdesc_table = PatdescSQL()
lawyer_table = LawyerSQL()
sciref_table = ScirefSQL()
usreldoc_table = UsreldocSQL()

xmlclasses = [AssigneeXML, CitationXML, ClassXML, InventorXML, \
              PatentXML, PatdescXML, LawyerXML, ScirefXML, UsreldocXML]

def get_year_list(yearstring):
    """
    Given a [yearstring] of forms
    year1
    year1-year2
    year1,year2,year3
    year1-year2,year3-year4
    Expands into a list of year integers, and returns
    """
    years = []
    for subset in yearstring.split(','):
       sublist = subset.split('-')
       start = int(sublist[0])
       end = int(sublist[1])+1 if len(sublist) > 1 else start+1
       years.extend(range(start,end))
    return years

def connect_client():
    """
    Loops for a minute until the Client connects to the local ipcluster
    """
    start=time.time()
    print 'Client connecting...'
    while time.time()-start < 300:
        try:
            c = Client()
            dview = c[:]
            break
        except:
            time.sleep(5)
            continue
    return dview

def run_parse():
    import parse
    import time
    import sys
    import itertools
    parsed_xmls = parse.parallel_parse(files)
    parsed_grants = parse.parse_patent(parsed_xmls)
    parse.build_tables(parsed_grants)
    return parse.get_inserts()

# TODO: these don't work
def run_clean(process_config):
    if process_config['clean']:
        print 'Running clean...'
        execfile('clean.py')

def run_consolidate(process_config):
    if process_config['consolidate']:
        print 'Running consolidate...'
        execfile('consolidate.py')

if __name__=='__main__':
    s = datetime.datetime.now()
    # accepts path to configuration file as command line option
    process_config, parse_config = get_config_options(sys.argv[1])

    # download the files to be parsed
    download_files(parse_config['downloaddir'], parse_config['years'])

    # find files
    print "Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()),parse_config['datadir'])
    files = parse.list_files(parse_config['datadir'],parse_config['dataregex'])
    print "Found {2} files matching {0} in directory {1}".format(parse_config['dataregex'], parse_config['datadir'], len(files))

    # connect to ipcluster and get config options
    dview = connect_client()
    dview.block=True
    dview.scatter('files',files)
    dview['process_config'] = process_config
    dview['parse_config'] = parse_config

    # run parse and commit SQL
    print 'Running parse...'
    inserts = list(itertools.chain.from_iterable(dview.apply(run_parse)))
    parse.commit_tables(inserts)
    f = datetime.datetime.now()
    print 'Finished parsing in {0}'.format(str(f-s))

    # run extra phases if needed, then move output files
    run_clean(process_config)
    run_consolidate(process_config)
    parse.move_tables(process_config['outputdir'])
