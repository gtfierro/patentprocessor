#!/usr/bin/env python

import logging
import os
import datetime
import re
import itertools
import sys
import lib.argconfig_parse as argconfig_parse
import lib.alchemy as alchemy
import shutil
from lib.config_parser import get_xml_handlers

xmlhandlers = get_xml_handlers('process.cfg')
logfile = "./" + 'xml-parsing.log'
logging.basicConfig(filename=logfile, level=logging.DEBUG)
commit_frequency = alchemy.get_config().get('parse').get('commit_frequency')

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
    Given a string [filename], opens the file and returns a generator
    that yields tuples. A tuple is of format (year, xmldoc string). A tuple
    is returned for every valid XML doc in [filename]
    """
    # search for terminating XML tag
    endtag_regex = re.compile('^<!DOCTYPE (.*) SYSTEM')
    endtag = ''
    with open(filename, 'r') as f:
        doc = '' # (re)initialize current XML doc to empty string
        for line in f:
            doc += line
            endtag = endtag_regex.findall(line) if not endtag else endtag
            if not endtag: continue
            terminate = re.compile('^</{0}>'.format(endtag[0]))
            if terminate.findall(line):
                yield (_get_date(filename), doc)
                endtag = ''
                doc = ''

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
    del xmltuple
    patobj = Patobj()
    for attr in ['pat','patent','app','assignee_list','inventor_list','lawyer_list',
                 'us_relation_list','us_classifications','ipcr_classifications',
                 'citation_list']:
        patobj.__dict__[attr] = getattr(patent,attr)
    return patobj

def parse_patents(xmltuples):
    """
    Given a list of xml strings as [xmltuples], parses them
    all and returns a flat iterator of patSQL.*XML instances
    """
    if not xmltuples: return []
    return map(parse_patent, xmltuples)

def database_commit(patobjects, commit_frequency=commit_frequency):
    """
    takes in a list of Patent objects (from parse_patents above) and commits
    them to the database. This method is designed to be used sequentially to
    account for db concurrency.  The optional argument `commit_frequency`
    determines the frequency with which we commit the objects to the database.
    If set to 0, it will commit after all patobjects have been added.  Setting
    `commit_frequency` to be low (but not 0) is helpful for low memory machines.
    """
    i = 0
    num_objs = len(patobjects) # compute this once
    for patobj in patobjects:
        i += 1
        alchemy.add(patobj)
        if commit_frequency and (i % commit_frequency == 0 or i == num_objs):
            alchemy.commit()
    if not commit_frequency:
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
    dbtype = alchemy.config.get('global', 'database')
    dbfile = alchemy.config.get(dbtype, 'database')
    try:
        shutil.move(dbfile,
                    '{0}/{1}'.format(output_directory, dbfile))
    except:
        print 'Database file {0} does not exist'.format(dbfile)


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
