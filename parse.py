#!/usr/bin/env python

import logging
import os
import datetime
import re
import mmap
import contextlib
import itertools
import sys
import lib.grant_handler
import lib.patSQL
import lib.argconfig_parse
sys.path.append( '.' )
sys.path.append( './lib/' )

xmlclasses = [lib.patSQL.AssigneeXML, lib.patSQL.CitationXML, lib.patSQL.ClassXML, \
              lib.patSQL.InventorXML, lib.patSQL.PatentXML, lib.patSQL.PatdescXML, \
              lib.patSQL.LawyerXML, lib.patSQL.ScirefXML, lib.patSQL.UsreldocXML]

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
    parsed = itertools.imap(parse_file, filelist)
    return itertools.chain.from_iterable(parsed)

def apply_xmlclass(us_patent_grant):
    parsed_grants = []
    try:
        patobj = lib.grant_handler.PatentGrant(us_patent_grant, True)
        for xmlclass in xmlclasses:
            parsed_grants.append(xmlclass(patobj))
    except Exception as inst:
        logging.error(type(inst))
        logging.error("  - Error parsing patent: %s" % (us_patent_grant[:400]))
    return parsed_grants

def parse_patent(grant_list):
    parsed_grants = itertools.imap(apply_xmlclass, grant_list)
    # errored patents return None; we want to get rid of these
    parsed_grants = itertools.ifilter(lambda x: x, parsed_grants)
    return itertools.chain.from_iterable(parsed_grants)

def build_tables(parsed_grants):
    for parsed_grant in parsed_grants:
        parsed_grant.insert_table()

def get_tables():
    return (lib.patSQL.assignee_table, lib.patSQL.citation_table, lib.patSQL.class_table, lib.patSQL.inventor_table,\
           lib.patSQL.patent_table, lib.patSQL.patdesc_table, lib.patSQL.lawyer_table, lib.patSQL.sciref_table,\
           lib.patSQL.usreldoc_table)

def get_inserts():
    return [(x, x.inserts) for x in get_tables()]

def commit_tables(collection):
    #for inserts in collection:
    for insert in collection:
        insert[0].commit(insert[1])

def move_tables(output_directory):
    """
    Moves the output sqlite3 files to the output directory
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    for database in ['assignee.sqlite3','citation.sqlite3','class.sqlite3',\
                     'inventor.sqlite3','patent.sqlite3','patdesc.sqlite3',\
                     'lawyer.sqlite3','sciref.sqlite3','usreldoc.sqlite3']:
        os.rename(database,output_directory+'/'+database)


def main(patentroot, xmlregex, verbosity, output_directory='.'):
    logging.basicConfig(filename=logfile, level=verbosity)
    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()),patentroot))
    files = list_files(patentroot, xmlregex)
    logging.info("Found all files matching {0} in directory {1}".format(xmlregex, patentroot))
    parsed_xmls = parallel_parse(files)
    logging.info("Extracted all individual XML files")
    parsed_grants = parse_patent(parsed_xmls)
    logging.info("Parsed all individual XML files")
    build_tables(parsed_grants)
    inserts = get_inserts()
    logging.info("SQL inserts queued up")
    commit_tables(inserts)
    logging.info("SQL tables committed")
    move_tables(output_directory)
    logging.info("SQL tables moved to {0}".format(output_directory))
    logging.info("Parse completed at {0}".format(str(datetime.datetime.today())))

if __name__ == '__main__':

    args = lib.argconfig_parse.ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()

    logfile = "./" + 'xml-parsing.log'
    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR)
