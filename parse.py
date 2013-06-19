#!/usr/bin/env python

import logging
import argparse
import os
import datetime
import re
import mmap
import contextlib
import multiprocessing
import itertools
from xml.sax import SAXException

import sys
sys.path.append( '.' )
sys.path.append( './lib/' )

from grant_handler import PatentGrant
from patSQL import *
from argconfig_parse import ArgHandler

xmlclasses = [AssigneeXML, CitationXML, ClassXML, InventorXML, \
              PatentXML, PatdescXML, LawyerXML, ScirefXML, UsreldocXML]

regex = re.compile(r"""([<][?]xml version.*?[>]\s*[<][!]DOCTYPE\s+([A-Za-z-]+)\s+.*?/\2[>])""", re.S+re.I)

def list_files(patentroot, xmlregex):
    """
    Returns listing of all files within patentroot
    whose filenames match xmlregex
    """
    files = [patentroot+'/'+fi for fi in os.listdir(patentroot) \
            if re.search(xmlregex, fi, re.I) != None]
    if not files:
        logging.error("No files matching {0} found in {1}".format(XMLREGEX,PATENTROOT))
        sys.exit(1)
    return files

def parse_file(filename):
    if not filename: return
    parsed_xmls = []
    size = os.stat(filename).st_size
    logging.debug("Parsing file: {0}".format(filename))
    with open(filename,'r') as f:
        with contextlib.closing(mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)) as m:
            res = [x[0] for x in regex.findall(m)]
            parsed_xmls.extend(res)
    return parsed_xmls

def parallel_parse(filelist):
    if not filelist: return
    #pool = multiprocessing.Pool(multiprocessing.cpu_count())
    parsed = itertools.imap(parse_file, filelist)
    return itertools.chain.from_iterable(parsed)

def apply_xmlclass(us_patent_grant):
    parsed_grants = []
    try:
        patobj = PatentGrant(us_patent_grant, True)
        for xmlclass in xmlclasses:
            parsed_grants.append(xmlclass(patobj))
    except Exception as inst:
        logging.error(type(inst))
        logging.error("  - Error parsing patent: %s" % (us_patent_grant[:400]))
    return parsed_grants

def parse_patent(grant_list):
    parsed_grants = itertools.imap(apply_xmlclass, grant_list)
    parsed_grants = itertools.ifilter(lambda x: x, parsed_grants)
    return itertools.chain.from_iterable(parsed_grants)

def load_sql(patent):
    patent.insert_table()

# TODO: unittest
def build_tables(parsed_grants):
    map(load_sql, parsed_grants)

def commit_tables():
    assignee_table.commit();
    citation_table.commit();
    class_table.commit();
    inventor_table.commit();
    patent_table.commit();
    patdesc_table.commit();
    lawyer_table.commit();
    sciref_table.commit();
    usreldoc_table.commit();

def main(patentroot, xmlregex, verbosity):
    logging.basicConfig(filename=logfile, level=verbosity)
    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()),patentroot))
    files = list_files(patentroot, xmlregex)
    logging.info("Found all files matching {0} in directory {1}".format(xmlregex, patentroot))
    parsed_xmls = parallel_parse(files)
    logging.info("Extracted all individual XML files")
    parsed_grants = parse_patent(parsed_xmls)
    logging.info("Parsed all individual XML files")
    build_tables(parsed_grants)
    logging.info("SQL inserts queued up")
    commit_tables()
    logging.info("SQL tables committed")
    logging.info("Parse completed at {0}".format(str(datetime.datetime.today())))

if __name__ == '__main__':

    args = ArgHandler(sys.argv[1:])

    if args.invalid_config():
        args.get_help()

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()

    logfile = "./" + 'xml-parsing.log'
    main(PATENTROOT, XMLREGEX, VERBOSITY)
