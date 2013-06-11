#!/usr/bin/env python

import os
import sys
import unittest
import logging
import re

sys.path.append('..')
sys.path.append('../lib')
import parse
from grant_handler import PatentGrant
from patSQL import *

basedir = os.path.dirname(__file__)
testdir = os.path.join(basedir, './fixtures/xml/')
testfileone = 'ipg120327.one.xml'
testfiletwo = 'ipg120327.two.xml'
regex = re.compile(r"""([<][?]xml version.*?[>]\s*[<][!]DOCTYPE\s+([A-Za-z-]+)\s+.*?/\2[>])""", re.S+re.I)

xmlclasses = [AssigneeXML, CitationXML, ClassXML, InventorXML, \
              PatentXML, PatdescXML, LawyerXML, ScirefXML, UsreldocXML]

class TestParseFile(unittest.TestCase):
    
    def setUp(self):
        pass

    def test_parse_file_one(self):
        parsed_output = parse.parse_file(testdir+testfileone)
        self.assertTrue(isinstance(parsed_output, list))
        self.assertTrue(len(parsed_output) == 1)
        self.assertTrue(isinstance(parsed_output[0], str))
        self.assertTrue(regex.match(parsed_output[0]))

    def test_parallel_parse_one(self):
        filelist = [testdir+testfileone]
        parsed_output = parse.parallel_parse(filelist)
        self.assertTrue(isinstance(parsed_output,list))
        self.assertTrue(len(parsed_output) == 1)
        self.assertTrue(isinstance(parsed_output[0], str))
        self.assertTrue(regex.match(parsed_output[0]))

    def test_parse_file_two(self):
        parsed_output = parse.parse_file(testdir+testfiletwo)
        self.assertTrue(isinstance(parsed_output, list))
        self.assertTrue(len(parsed_output) == 2)
        self.assertTrue(isinstance(parsed_output[0], str))
        self.assertTrue(isinstance(parsed_output[1], str))
        self.assertTrue(regex.match(parsed_output[0]))
        self.assertTrue(regex.match(parsed_output[1]))

    def test_parallel_parse_two(self):
        filelist = [testdir+testfiletwo]
        parsed_output = parse.parallel_parse(filelist)
        self.assertTrue(isinstance(parsed_output,list))
        self.assertTrue(len(parsed_output) == 2)
        self.assertTrue(isinstance(parsed_output[0], str))
        self.assertTrue(isinstance(parsed_output[1], str))
        self.assertTrue(regex.match(parsed_output[0]))
        self.assertTrue(regex.match(parsed_output[1]))
    
    def test_use_parallel_parse_one(self):
        filelist = [testdir+testfileone]
        parsed_output = parse.parallel_parse(filelist)
        patobj = PatentGrant(parsed_output[0], True)
        parsed_xml = [xmlclass(patobj) for xmlclass in xmlclasses]
        self.assertTrue(len(parsed_xml) == len(xmlclasses))
        self.assertTrue(all(parsed_xml))

    def test_use_parallel_parse_two(self):
        filelist = [testdir+testfiletwo]
        parsed_output = parse.parallel_parse(filelist)
        parsed_xml = []
        for us_patent_grant in parsed_output:
            self.assertTrue(isinstance(us_patent_grant, str))
            patobj = PatentGrant(us_patent_grant, True)
            for xmlclass in xmlclasses:
                parsed_xml.append(xmlclass(patobj))
        self.assertTrue(len(parsed_xml) == 2 * len(xmlclasses))
        self.assertTrue(all(parsed_xml))
    
    def test_list_files(self):
        patentroot = '.'
        testdir = [os.path.join(basedir, './fixtures/xml')]
        xmlregex = r'ipg120327.one.xml'
        files = parse.list_files(testdir, patentroot, xmlregex)
        self.assertTrue(isinstance(files, list))
        self.assertTrue(len(files) == 1)
        self.assertTrue(all(filter(lambda x: isinstance(x, str), files)))
        self.assertTrue(all(map(lambda x: os.path.exists(x), files)))

    def test_parse_patent(self):
        patentroot = '.'
        testdir = [os.path.join(basedir, './fixtures/xml')]
        xmlregex = r'ipg120327.one.xml'
        filelist = parse.list_files(testdir, patentroot, xmlregex)
        grant_list = parse.parallel_parse(filelist)
        parsed_grants = parse.parse_patent(grant_list)
        self.assertTrue(len(list(parsed_grants)) == len(grant_list)*len(xmlclasses))


if __name__ == '__main__':
    unittest.main()
