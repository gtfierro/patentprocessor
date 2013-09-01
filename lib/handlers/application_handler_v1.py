#!/usr/bin/env python

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

from cStringIO import StringIO
from datetime import datetime
from unidecode import unidecode
import re
import uuid
import xml.sax
import xml_util
import xml_driver

claim_num_regex = re.compile(r'^\d+\. *') # removes claim number from claim text


class PatentGrant(object):

    def __init__(self, xml_string, is_string=False):
        xh = xml_driver.XMLHandler()
        parser = xml_driver.make_parser()

        parser.setContentHandler(xh)
        parser.setFeature(xml_driver.handler.feature_external_ges, False)
        l = xml.sax.xmlreader.Locator()
        xh.setDocumentLocator(l)
        if is_string:
            parser.parse(StringIO(xml_string))
        else:
            parser.parse(xml_string)
        self.xml = xh.root.us_patent_application

        self.country = self.xml.publication_reference.contents_of('country', upper=False)[0]
        self.patent = xml_util.normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        self.date_applied = self.xml.publication_reference.contents_of('date')[0] # changed from grant to applied
        self.pat_type = None # not sure exactly what this is
        # don't know what to do about referencing a patent grant
        # or when the patent has been granted
        #
        #
        #
        #
        #
        self.clm_num = len(self.xml.claims.claim)
        self.abstract = self.xml.abstract.contents_of('p', '', as_string=True, upper=False)
        self.invention_title = self.xml.us_bibliographic_data_application.contents_of('invention_title')

        self.pat = {
            "id": self.patent,
            "type": self.pat_type,
            "number": self.patent,
            "country": self.country,
            "date": self._fix_date(self.date_grant),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "num_claims": self.clm_num
        }

        def _fix_date(self, datestring):
            """
            Converts a number representing YY/MM to a Date
            """
            if not datestring:
                return None
            elif datestring[:4] < "1900":
                return None
            # default to first of month in absence of day
            if datestring[-4:-2] == '00':
                datestring = datestring[:-4] + '01' + datestring[-2:]
            if datestring[-2:] == '00':
                datestring = datestring[:6] + '01'
            try:
                datestring = datetime.strptime(datestring, '%Y%m%d')
                return datestring
            except Exception as inst:
                print inst, datestring
                return None