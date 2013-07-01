#!/usr/bin/env python

import logging
import os
import datetime
import re
import mmap
import contextlib
import itertools
import sys
import lib.grant_handler as grant_handler 
import lib.patSQL as patSQL
import lib.argconfig_parse as argconfig_parse

xmlclasses = [patSQL.AssigneeXML, patSQL.CitationXML, patSQL.ClassXML, \
              patSQL.InventorXML, patSQL.PatentXML, patSQL.PatdescXML, \
              patSQL.LawyerXML, patSQL.ScirefXML, patSQL.UsreldocXML]


def xml_gen(obj):
    """
    XML generator for iteration of the large XML file
    (otherwise high memory required) in replacement of RE
    """
    data = []
    for rec in obj:
        data.append(rec)
        if rec.find("</us-patent-grant>") >= 0:
            yield "".join(data)
            data = []


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


def parse_file(filename):
    if not filename:
        return
    parsed_xmls = []
    size = os.stat(filename).st_size
    logging.debug("Parsing file: {0}".format(filename))
    with open(filename, 'r') as f:
        with contextlib.closing(mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)) as m:
            res = [x[0] for x in regex.findall(m)]
            parsed_xmls.extend(res)
    return parsed_xmls


def parallel_parse(filelist):
    if not filelist:
        return
    parsed = itertools.imap(parse_file, filelist)
    return itertools.chain.from_iterable(parsed)


def apply_xmlclass(us_patent_grant):
    parsed_grants = []
    try:
        patobj = grant_handler.PatentGrant(us_patent_grant, True)
        for xmlclass in xmlclasses:
            parsed_grants.append(xmlclass(patobj))
    #except Exception as inst:
    #    logging.error(type(inst))
    #    logging.error("  - Error parsing patent: %s" % (us_patent_grant[:400]))
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
    logging.basicConfig(filename=logfile, level=verbosity)

    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.datetime.today()), patentroot))
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


    args = argconfig_parse.ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()

    logfile = "./" + 'xml-parsing.log'
    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR)
