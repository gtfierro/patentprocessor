#!/usr/bin/env python

import logging
import os
import datetime
import re
import mmap
import contextlib
import itertools
import sys
import lib.handlers.grant_handler as grant_handler
import lib.patSQL as patSQL
import lib.argconfig_parse as argconfig_parse
from lib.config_parser import get_xml_handlers

xmlclasses = [patSQL.AssigneeXML, patSQL.CitationXML, patSQL.ClassXML, \
              patSQL.InventorXML, patSQL.PatentXML, patSQL.PatdescXML, \
              patSQL.LawyerXML, patSQL.ScirefXML, patSQL.UsreldocXML]

regex = re.compile(r"""([<][?]xml version.*?[>]\s*[<][!]DOCTYPE\s+([A-Za-z-]+)\s+.*?/\2[>])""", re.S+re.I)
xmlhandlers = get_xml_handlers('process.cfg')

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

def _get_date(filename, dateformat='ipg%y%m%d.xml'):
    """
    Given a [filename], returns the expanded year.
    The optional [dateformat] argument allows for different file formats
    """
    filename = re.search(r'ipg\d{6}',filename)
    if not filename: return 'default'
    filename = filename.group() + '.xml'
    dateobj = datetime.datetime.strptime(filename, dateformat)
    return int(dateobj.strftime('%Y%m%d')) # returns YYYYMMDD

def _get_parser(date):
    """
    Given a [date], returns the class of parser needed
    to parse it
    """
    for daterange in xmlhandlers.iterkeys():
        if daterange[0] <= date <= daterange[1]:
            return xmlhandlers[daterange]

def extract_xml_strings(filename):
    """
    Given a [filename], opens the file using mmap and returns a list of tuples.
    Each tuple is of format (year, xml doc string). A tuple is returned for
    every valid XML doc in [filename]
    """
    if not filename: return
    parsed_xmls = []
    size = os.stat(filename).st_size
    logging.debug("Parsing file: {0}".format(filename))
    with open(filename,'r') as f:
        with contextlib.closing(mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)) as m:
            res = [(_get_date(filename), x[0]) for x in regex.findall(m)]
            parsed_xmls.extend(res)
    return parsed_xmls

def parse_files(filelist):
    """
    Given a list of files, extracts the XML strings from each and returns a
    flat iterable of all of them.
    """
    if not filelist: return
    parsed = itertools.imap(extract_xml_strings, filelist)
    return itertools.chain.from_iterable(parsed)

def apply_xmlclass(xmltuple):
    """
    Parses an xml string given as [xmltuple] with the appropriate parser (given
    by the first part of the tuple) and returns the patSQL.*XML formulations of
    it. Expect this to change when we integrate Ron's SQLAlchemy stuff
    """
    parsed_grants = []
    try:
        date, xml = xmltuple # extract out the parts of the tuple
        patobj = _get_parser(date).PatentGrant(xml, True)
        for xmlclass in xmlclasses:
            parsed_grants.append(xmlclass(patobj))
    except Exception as inst:
        logging.error(type(inst))
        logging.error("  - Error parsing patent: %s" % (xml[0][:400]))
    return parsed_grants

def parse_patents(xmltuples):
    """
    Given a list of xml strings as [xmltuples], parses them
    all and returns a flat iterator of patSQL.*XML instances
    """
    parsed_grants = itertools.imap(apply_xmlclass, xmltuples)
    # errored patents return None; we want to get rid of these
    parsed_grants = itertools.ifilter(lambda x: x, parsed_grants)
    return itertools.chain.from_iterable(parsed_grants)

def build_tables(parsed_grants):
    for parsed_grant in parsed_grants:
        parsed_grant.insert_table()

def get_tables():
    return (patSQL.assignee_table, patSQL.citation_table, patSQL.class_table, patSQL.inventor_table,\
           patSQL.patent_table, patSQL.patdesc_table, patSQL.lawyer_table, patSQL.sciref_table,\
           patSQL.usreldoc_table)

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
    parsed_xmls = parse_files(files)
    logging.info("Extracted all individual XML files")
    parsed_grants = parse_patents(parsed_xmls)
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

    args = argconfig_parse.ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()

    logfile = "./" + 'xml-parsing.log'
    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR)
