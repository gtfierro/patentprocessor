#!/usr/bin/env python

import sys
import os
import argparse
import logging

class ArgHandler(object):

    def __init__(self, arglist):
        self.arglist = arglist

        # setup argparse
        self.parser = argparse.ArgumentParser(description=\
                'Specify source directory/directories for xml files to be parsed')
        self.parser.add_argument('--patentroot','-p', type=str, nargs='?',
                default=os.environ['PATENTROOT'] \
                if os.environ.has_key('PATENTROOT') else '.',
                help='root directory of all patent files')
        self.parser.add_argument('--xmlregex','-x', type=str,
                nargs='?', default=r"ipg\d{6}.xml",
                help='regex used to match xml files in the PATENTROOT directory.\
                     Defaults to ipg\d{6}.xml')
        self.parser.add_argument('--verbosity', '-v', type = int,
                nargs='?', default=0,
                help='Set the level of verbosity for the computation. The higher the \
                verbosity level, the less restrictive the print policy. 0 (default) \
                = error, 1 = warning, 2 = info, 3 = debug')
        self.parser.add_argument('--output-directory', '-o', type=str, nargs='?',
                default=os.environ['PATENTOUTPUTDIR'] \
                if os.environ.has_key('PATENTOUTPUTDIR') else '.',
                help='Set the output directory for the resulting sqlite3 files. Defaults\
                     to the current directory "."')
        self.parser.add_argument('--document-type', '-d', type=str, nargs='?', 
                default='grant',
                help='Choose whether your files are patent grants or applications. \
                     Defaults to patent grants.')

        # parse arguments and assign values
        args = self.parser.parse_args(self.arglist)
        self.xmlregex = args.xmlregex
        self.patentroot = args.patentroot
        self.output_directory = args.output_directory
        self.document_type = args.document_type

        # adjust verbosity levels based on specified input
        logging_levels = {0: logging.ERROR,
                          1: logging.WARNING,
                          2: logging.INFO,
                          3: logging.DEBUG}
        self.verbosity = logging_levels[args.verbosity]

    def get_xmlregex(self):
        return self.xmlregex

    def get_patentroot(self):
        return self.patentroot

    def get_verbosity(self):
        return self.verbosity

    def get_output_directory(self):
        return self.output_directory

    def get_document_type(self):
        return self.document_type

    def get_help(self):
        self.parser.print_help()
        sys.exit(1)
