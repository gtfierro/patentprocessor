#!/usr/bin/env python

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

from cStringIO import StringIO
from datetime import datetime
from unidecode import unidecode
from handler import Patobj, PatentHandler
import re
import uuid
import xml.sax
import xml_util
import xml_driver

claim_num_regex = re.compile(r'^\d+\. *') # removes claim number from claim text


class Patent(PatentHandler):

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

        self.attributes = ['app','application','assignee_list']

        self.xml = xh.root.patent_application_publication

        if self.xml.foreign_priority_data:
            self.country = self.xml.foreign_priority_data.contents_of('country_code')[0]
        else:
            self.country = self.xml.country_code[0]
        self.application = xml_util.normalize_document_identifier(self.xml.document_id.contents_of('doc_number')[0])
        self.kind = self.xml.document_id.contents_of('kind_code')[0]
        self.pat_type = None
        self.date_app = self.xml.document_id.contents_of('document_date')[0]
        self.clm_num = len(self.xml.subdoc_claims.claim)
        self.abstract = self.xml.subdoc_abstract.contents_of('paragraph', '', as_string=True, upper=False)
        self.invention_title = self._invention_title()

        self.app = {
            "id": self.application,
            "type": self.pat_type,
            "number": self.application,
            "country": self.country,
            "date": self._fix_date(self.date_app),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "num_claims": self.clm_num
        }
        self.app["id"] = str(self.app["date"])[:4] + "/" + self.app["number"]

        print(self.assignee_list)

    def _invention_title(self):
        original = self.xml.contents_of('title_of_invention', upper=False)[0]
        if isinstance(original, list):
            original = ''.join(original)
        return original

    def _name_helper(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True, upper=False)
        lastname = tag_root.contents_of('last_name', as_string=True, upper=False)
        return xml_util.associate_prefix(firstname, lastname)

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True, upper=False)
        lastname = tag_root.contents_of('last_name', as_string=True, upper=False)
        firstname, lastname = xml_util.associate_prefix(firstname, lastname)
        return {'name_first': firstname, 'name_last': lastname}

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

    @property
    def assignee_list(self):
        """
        Returns list of dictionaries:
        assignee:
          name_last
          name_first
          residence
          nationality
          organization
          sequence
        location:
          id
          city
          state
          country
        """
        assignees = self.xml.assignee
        if not assignees:
            return []
        res = []
        for i, assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee))  # add firstname, lastname
            asg['organization'] = assignee.contents_of('organization_name', as_string=True, upper=False)
            asg['role'] = assignee.contents_of('role', as_string=True)
            asg['nationality'] = assignee.contents_of('country_code')[0]
            asg['residence'] = assignee.contents_of('country_code')[0]
            # add location data for assignee
            loc = {}
            for tag in ['city', 'state']:
                loc[tag] = assignee.contents_of(tag, as_string=True, upper=False)
            loc['country'] = asg['nationality']
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode(u"|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(asg.values()) or any(loc.values()):
                asg['sequence'] = i
                asg['uuid'] = str(uuid.uuid1())
                res.append([asg, loc])
        return res
