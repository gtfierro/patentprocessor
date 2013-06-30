#!/usr/bin/env python

import os
import re
import sys
import logging
sys.path.append('.')
sys.path.append('./lib/')

import alchemy
from argconfig_parse import ArgHandler
from datetime import datetime
from grant_handler import PatentGrant


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


def main(patentroot, xmlregex, verbosity, output_directory='.'):
    logging.basicConfig(filename=logfile, level=verbosity)
    logging.info("Starting parse on {0} on directory {1}".format(str(datetime.today()), patentroot))

    """
    Returns listing of all files within patentroot
    whose filenames match xmlregex
    """
    files = [patentroot+'/'+fi for fi in os.listdir(patentroot)
             if re.search(xmlregex, fi, re.I) is not None]
    if not files:
        logging.error("No files matching {0} found in {1}".format(xmlregex, patentroot))
        sys.exit(1)

    for filename in files:
        t = datetime.now()
        for i, xml_string in enumerate(xml_gen(open(filename, "rb"))):
            try:
                patobj = PatentGrant(xml_string)
            except Exception as inst:
                print " *", inst
            if patobj:
                alchemy.add(patobj, override=False)
            if i % 1000 == 0:
                print " *", datetime.now() - t, "- rec:", i
                alchemy.commit()
        print filename, datetime.now() - t


if __name__ == '__main__':
    args = ArgHandler(sys.argv[1:])

    XMLREGEX = args.get_xmlregex()
    PATENTROOT = args.get_patentroot()
    VERBOSITY = args.get_verbosity()
    PATENTOUTPUTDIR = args.get_output_directory()

    logfile = "./" + 'xml-parsing.log'
    main(PATENTROOT, XMLREGEX, VERBOSITY, PATENTOUTPUTDIR)
