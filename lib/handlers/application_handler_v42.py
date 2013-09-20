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

        self.attributes = ['app','application','assignee_list','inventor_list',\
                     'us_relation_list','us_classifications','ipcr_classifications',
                     'claims']

        self.xml = xh.root.us_patent_application

        self.country = self.xml.publication_reference.contents_of('country', upper=False)[0]
        self.application = xml_util.normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        self.date_app = self.xml.publication_reference.contents_of('date')[0]
        if self.xml.application_reference:
            self.pat_type = self.xml.application_reference[0].get_attribute('appl-type', upper=False)
        else:
            self.pat_type = None
        self.clm_num = len(self.xml.claims.claim)
        self.abstract = self.xml.abstract.contents_of('p', '', as_string=True, upper=False)
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

    def _invention_title(self):
        original = self.xml.contents_of('invention_title', upper=False)[0]
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
        assignees = self.xml.assignees.assignee
        if not assignees:
            return []
        res = []
        for i, assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee))  # add firstname, lastname
            asg['organization'] = assignee.contents_of('orgname', as_string=True, upper=False)
            asg['role'] = assignee.contents_of('role', as_string=True)
            asg['nationality'] = assignee.nationality.contents_of('country')[0]
            asg['residence'] = assignee.nationality.contents_of('country')[0]
            # add location data for assignee
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = assignee.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode(u"|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(asg.values()) or any(loc.values()):
                asg['sequence'] = i
                asg['uuid'] = str(uuid.uuid1())
                res.append([asg, loc])
        return res

    @property
    def inventor_list(self):
        """
        Returns list of lists of applicant dictionary and location dictionary
        applicant:
          name_last
          name_first
          nationality
          sequence
        location:
          id
          city
          state
          country
        """
        applicants = self.xml.applicants.applicant
        if not applicants:
            return []
        res = []
        for i, applicant in enumerate(applicants):
            # add applicant data
            app = {}
            app.update(self._name_helper_dict(applicant.addressbook))
            app['nationality'] = applicant.nationality.contents_of('country', as_string=True)
            # add location data for applicant
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = applicant.addressbook.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(app.values()) or any(loc.values()):
                app['sequence'] = i
                app['uuid'] = str(uuid.uuid1())
                res.append([app, loc])
        return res

    def _get_doc_info(self, root):
        """
        Accepts an XMLElement root as an argument. Returns list of
        [country, doc-number, kind, date] for the given root
        """
        res = {}
        for tag in ['country', 'kind', 'date']:
            data = root.contents_of(tag)
            res[tag] = data[0] if data else ''
        res['number'] = xml_util.normalize_document_identifier(
            root.contents_of('doc_number')[0])
        return res

    @property
    def us_relation_list(self):
        """
        returns list of dictionaries for us reldoc:
        usreldoc:
          doctype
          status (parent status)
          date
          number
          kind
          country
          relationship
          sequence
        """
        root = self.xml.us_related_documents
        if not root:
            return []
        root = root[0]
        res = []
        i = 0
        for reldoc in root.children:
            if reldoc._name == 'related_publication' or \
               reldoc._name == 'us_provisional_application':
                data = {'doctype': reldoc._name}
                data.update(self._get_doc_info(reldoc))
                data['date'] = self._fix_date(data['date'])
                if any(data.values()):
                    data['sequence'] = i
                    data['uuid'] = str(uuid.uuid1())
                    i = i + 1
                    res.append(data)
            for relation in reldoc.relation:
                for relationship in ['parent_doc', 'parent_grant_document',
                                     'parent_pct_document', 'child_doc']:
                    data = {'doctype': reldoc._name}
                    doc = getattr(relation, relationship)
                    if not doc:
                        continue
                    data.update(self._get_doc_info(doc[0]))
                    data['date'] = self._fix_date(data['date'])
                    data['status'] = doc[0].contents_of('parent_status', as_string=True)
                    data['relationship'] = relationship  # parent/child
                    if any(data.values()):
                        data['sequence'] = i
                        data['uuid'] = str(uuid.uuid1())
                        i = i + 1
                        res.append(data)
        return res

    @property
    def us_classifications(self):
        """
        Returns list of dictionaries representing us classification
        main:
          class
          subclass
        """
        classes = []
        i = 0
        main = self.xml.classification_national.contents_of('main_classification')
        data = {'class': main[0][:3].replace(' ', ''),
                'subclass': main[0][3:].replace(' ', '')}
        if any(data.values()):
            classes.append([
                {'uuid': str(uuid.uuid1()), 'sequence': i},
                {'id': data['class'].upper()},
                {'id': "{class}/{subclass}".format(**data).upper()}])
            i = i + 1
        if self.xml.classification_national.further_classification:
            further = self.xml.classification_national.contents_of('further_classification')
            for classification in further:
                data = {'class': classification[:3].replace(' ', ''),
                        'subclass': classification[3:].replace(' ', '')}
                if any(data.values()):
                    classes.append([
                        {'uuid': str(uuid.uuid1()), 'sequence': i},
                        {'id': data['class'].upper()},
                        {'id': "{class}/{subclass}".format(**data).upper()}])
                    i = i + 1
        return classes

    @property
    def ipcr_classifications(self):
        """
        Returns list of dictionaries representing ipcr classifications
        ipcr:
          ipc_version_indicator
          classification_level
          section
          class
          subclass
          main_group
          subgroup
          symbol_position
          classification_value
          action_date
          classification_status
          classification_data_source
          sequence
        """
        ipcr_classifications = self.xml.classifications_ipcr
        if not ipcr_classifications:
            return []
        res = []
        # we can safely use [0] because there is only one ipcr_classifications tag
        for i, ipcr in enumerate(ipcr_classifications.classification_ipcr):
            data = {}
            for tag in ['classification_level', 'section',
                        'class', 'subclass', 'main_group', 'subgroup', 'symbol_position',
                        'classification_value', 'classification_status',
                        'classification_data_source']:
                data[tag] = ipcr.contents_of(tag, as_string=True)
            data['ipc_version_indicator'] = self._fix_date(ipcr.ipc_version_indicator.contents_of('date', as_string=True))
            data['action_date'] = self._fix_date(ipcr.action_date.contents_of('date', as_string=True))
            if any(data.values()):
                data['sequence'] = i
                data['uuid'] = str(uuid.uuid1())
                res.append(data)
        return res

    @property
    def claims(self):
        """
        Returns list of dictionaries representing claims
        claim:
          text
          dependent -- -1 if an independent claim, else this is the number
                       of the claim this one is dependent on
          sequence
        """
        claims = self.xml.claim
        res = []
        for i, claim in enumerate(claims):
            data = {}
            data['text'] = claim.contents_of('claim_text', as_string=True, upper=False)
            # remove leading claim num from text
            data['text'] = claim_num_regex.sub('', data['text'])
            data['sequence'] = i+1 # claims are 1-indexed
            if claim.claim_ref:
                # claim_refs are 'claim N', so we extract the N
                data['dependent'] = int(claim.contents_of('claim_ref',\
                                        as_string=True).split(' ')[-1])
            data['uuid'] = str(uuid.uuid1())
            res.append(data)
        return res
