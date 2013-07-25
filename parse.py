#!/usr/bin/env python

import logging
import os
import datetime
import re
import mmap
import contextlib
import itertools
import sys
import lib.argconfig_parse as argconfig_parse
import lib.alchemy as alchemy
from lib.config_parser import get_xml_handlers

regex = re.compile(r"""([<][?]xml version.*?[>]\s*[<][!]DOCTYPE\s+([A-Za-z-]+)\s+.*?/\2[>])""", re.S+re.I)
xmlhandlers = get_xml_handlers('process.cfg')
logfile = "./" + 'xml-parsing.log'
logging.basicConfig(filename=logfile, level=logging.DEBUG)

class Patobj(object): pass

def list_files(patentroot, xmlregex):
    """
    Returns listing of all files within patentroot
    whose filenames match xmlregex
    """
    files = [patentroot+'/'+fi for fi in os.listdir(patentroot)
             if re.search(xmlregex, fi, re.I) is not None]
    if not files:
        logging.error("No files matching {0} found in {1}".format(XMLREGEX, PATENTROOT))
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
    return xmlhandlers['default']

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
    with open(filename, 'r') as f:
        with contextlib.closing(mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)) as m:
            res = [(_get_date(filename), x[0]) for x in regex.findall(m)]
            parsed_xmls.extend(res)
    return parsed_xmls

def parse_files(filelist):
    """
    Given a list of files, extracts the XML strings from each and returns a
    flat iterable of all of them.
    """
    if not filelist: return []
    parsed = itertools.imap(extract_xml_strings, filelist)
    return itertools.chain.from_iterable(parsed)

def parse_patent(xmltuple):
    """
    Parses an xml string given as [xmltuple] with the appropriate parser (given
    by the first part of the tuple). Returns list of objects
    to be inserted into the database using SQLAlchemy
    """
    if not xmltuple: return
    try:
        date, xml = xmltuple # extract out the parts of the tuple
        patent = _get_parser(date).PatentGrant(xml, True)
    except Exception as inst:
        logging.error(inst)
        logging.error("  - Error parsing patent: %s" % (xml[:400]))
    patobj = Patobj()
    patobj.__dict__['pat'] = patent.pat
    patobj.__dict__['patent'] = patent.patent
    patobj.__dict__['app'] = patent.app
    patobj.__dict__['assignee_list'] = patent.assignee_list
    patobj.__dict__['inventor_list'] = patent.inventor_list
    patobj.__dict__['lawyer_list'] = patent.lawyer_list
    patobj.__dict__['us_relation_list'] = patent.us_relation_list
    patobj.__dict__['us_classifications'] = patent.us_classifications
    patobj.__dict__['ipcr_classifications'] = patent.ipcr_classifications
    patobj.__dict__['citation_list'] = patent.citation_list
    return patobj

def parse_patents(xmltuples):
    """
    Given a list of xml strings as [xmltuples], parses them
    all and returns a flat iterator of patSQL.*XML instances
    """
    if not xmltuples: return []
    return map(parse_patent, xmltuples)

def database_commit(patobjects):
    """
    takes in a list of Patent objects (from parse_patents above)
    and commits them to the database. This method is designed
    to be use sequentially to account for db concurrency
    """
    for patobj in patobjects:
        alchemy.add(patobj, temp=False)
    alchemy.commit()

# TODO: this should only move alchemy.sqlite3
def move_tables(output_directory):
    """
    Moves the output sqlite3 files to the output directory
    """
    if output_directory == ".":
        return
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    # RL modified >>>>>>
    for database in ['assignee', 'citation', 'class',
                     'inventor', 'patent', 'patdesc',
                     'lawyer', 'sciref', 'usreldoc']:
        shutil.move("{0}.sqlite3".format(database),
                    "{0}/{1}.sqlite3".format(output_directory, database))
    # <<<<<<


def main(patentroot, xmlregex, verbosity, output_directory='.'):
    logfile = "./" + 'xml-parsing.log'
    logging.basicConfig(filename=logfile, level=verbosity)

    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()), patentroot))
    files = list_files(patentroot, xmlregex)

    logging.info("Found all files matching {0} in directory {1}".format(xmlregex, patentroot))
    parsed_xmls = parse_files(files)
    logging.info("Extracted all individual XML files")
    patobjects = parse_patents(parsed_xmls)
    database_commit(patobjects)
    move_tables(output_directory)

    logging.info("SQL tables moved to {0}".format(output_directory))
    logging.info("Parse completed at {0}".format(str(datetime.datetime.today())))

if __name__ == '__main__':
    args = argconfig_parse.ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()

    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR)
