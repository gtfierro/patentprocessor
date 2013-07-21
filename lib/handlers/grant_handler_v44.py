#!/usr/bin/env python

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

from cStringIO import StringIO
from datetime import datetime
from unidecode import unidecode
import uuid
import xml.sax
import xml_util
import xml_driver


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
        self.xml = xh.root.us_patent_grant.us_bibliographic_data_grant

        self.country = self.xml.publication_reference.contents_of('country', upper=False)[0]
        self.patent = xml_util.normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        self.date_grant = self.xml.publication_reference.contents_of('date')[0]
        if self.xml.application_reference:
            self.pat_type = self.xml.application_reference[0].get_attribute('appl-type', upper=False)
        else:
            self.pat_type = None
        self.date_app = self.xml.application_reference.contents_of('date')[0]
        self.country_app = self.xml.application_reference.contents_of('country')[0]
        self.patent_app = self.xml.application_reference.contents_of('doc_number')[0]
        self.code_app = self.xml.contents_of('us_application_series_code')[0]
        self.clm_num = self.xml.contents_of('number_of_claims')[0]
        self.classes = self._classes()
        self.abstract = xh.root.us_patent_grant.abstract.contents_of('p', '', as_string=True, upper=False)
        self.invention_title = self._invention_title()

        # To depreciate >>>>>>
        self.asg_list = self._asg_list()
        self.cit_list = self._cit_list()
        self.rel_list = self._rel_list()
        self.inv_list = self._inv_list()
        self.law_list = self._law_list()
        # <<<<<<

        # To consolidate with above? >>>>>>
        self.pat = {
            "id": self.patent,
            "type": self.pat_type,
            "number": self.patent,
            "country": self.country,
            "date": self._fix_date(self.date_grant),
            "abstract": self.abstract,
            "title": self.invention_title,
            "kind": self.kind,
            "claims": self.clm_num
        }
        self.app = {
            "uuid": str(uuid.uuid1()),
            "type": self.code_app,
            "number": self.patent_app,
            "country": self.country_app,
            "date": self._fix_date(self.date_app)
        }
        # <<<<<<

    def _invention_title(self):
        original = self.xml.contents_of('invention_title', upper=False)[0]
        if isinstance(original, list):
            original = ''.join(original)
        return original

    def _classes(self):
        main = self.xml.classification_national.contents_of('main_classification')
        further = self.xml.classification_national.contents_of('further_classification')
        it = [main[0] if xml_util.has_content(main) else []]
        if xml_util.has_content(further):
            it.extend(further)
        if not it or not it[0]:
            return []
        else:
            return [[x[:3].replace(' ', ''), x[3:].replace(' ', '')] for x in it]

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

    # To depreciate >>>>>>
    def _asg_list(self):
        doc = self.xml.assignees.assignee
        data = []
        if not doc:
            return []
        if doc.first_name:
            data = [1]
            firstname, lastname = self._name_helper(doc)
            data.extend(lastname)
            data.extend(firstname)
        else:
            data = [0]
            data.extend(doc.contents_of('orgname'))
            data.extend(doc.contents_of('role'))
        for tag in ['street', 'city', 'state', 'country', 'postcode']:
            data.extend(doc.addressbook.address.contents_of(tag))
        data.extend(doc.nationality.contents_of('country'))
        data.extend(doc.residence.contents_of('country'))
        return [data]

    def _cit_list(self):
        res = []
        citations = self.xml.us_references_cited.us_citation
        for citation in citations:
            cit_data = citation.contents_of('category')
            if citation.patcit:
                for tag in ['country', 'doc_number', 'date', 'kind', 'name']:
                    contents = citation.contents_of(tag)
                    if isinstance(contents, list) and contents:
                        cit_data.append(contents[0])
                    else:
                        cit_data.append(contents if xml_util.has_content(contents) else '')
                cit_data.append('')
            if citation.othercit:
                contents = citation.contents_of('othercit')
                for chunk in contents:
                    cit_data.extend(['', '', '', '', ''])
                    if isinstance(chunk, list):
                        cit_data.append(''.join([xml_util.escape_html_nosub(x) for x in chunk]).upper())
                    else:
                        cit_data.append(xml_util.escape_html_nosub(chunk))
            res.append(cit_data)
        return res

    def _rel_helper(self, base, roots, taglist):
        """
        Given a list of XMLElements as the [roots], look for each of the tags
        in [taglist] and create a list of the contents of the tags for each of
        the roots. Starts each of the content lists with [base]
        """
        res = []
        for root in roots:
            data = base
            for tag in taglist:
                contents = root.contents_of(tag, default=[''])
                data.extend(contents[:1] if isinstance(contents, list) else [contents])
            res.append(data)
        return res

    def _rel_list(self):
        res = []
        taglist = ['doc_number', 'country', 'kind']
        for tag in ['continuation_in_part', 'continuation', 'division', 'reissue']:
            main = self.xml.__getattr__(tag)
            if not main:
                continue
            tag = tag.replace('_', '-').upper()
            relations = main.relation  # get all relations
            for relation in relations:
                if relation.child_doc:
                    res.extend(self._rel_helper([tag, -1], relation.child_doc, taglist))
                base = [tag, 1]
                taglist.extend(['date', 'parent_status'])
                if relation.parent_doc:
                    res.extend(self._rel_helper(base, relation.parent_doc, taglist))
                if relation.parent_doc.parent_grant_document:
                    res.extend(self._rel_helper(base, relation.parent_doc.parent_grant_document, taglist))
                if relation.parent_doc.parent_pct_document:
                    res.extend(self._rel_helper(base, relation.parent_doc.parent_pct_document, taglist))
            if res:
                break
        for tag in ['related-publication', 'us-provisional-application']:
            if not self.xml.__getattr__(tag):
                continue
            if self.xml.document_id:
                tmp = [tag, 0]
                for nested in ['doc_number', 'country', 'kind']:
                    tmp.extend(self.xml.document_id.contents_of(nested))
                res.append(tmp)
            if res:
                break
        return res

    def _inv_list(self):
        inventors = self.xml.inventors.inventor
        if not inventors:
            return []
        res = []
        for inventor in inventors:
            data = []
            firstname, lastname = self._name_helper(inventor.addressbook)
            data.append(lastname)
            data.append(firstname)
            for tag in ['street', 'city', 'state', 'country', 'postcode']:
                data.append(inventor.addressbook.address.contents_of(tag, as_string=True))
            data.append(inventor.nationality.contents_of('country', as_string=True))
            data.append(inventor.residence.contents_of('country', as_string=True))
            res.append(data)
        return res

    def _law_list(self):
        lawyers = self.xml.agents.agent
        if not lawyers:
            return []
        res = []
        for lawyer in lawyers:
            data = []
            firstname, lastname = self._name_helper(lawyer)
            data.append(lastname)
            data.append(firstname)
            data.append(lawyer.contents_of('country', as_string=True))
            data.append(lawyer.contents_of('orgname', as_string=True))
            res.append(data)
        return res
    # <<<<<<

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
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(asg.values()) or any(loc.values()):
                asg['sequence'] = i
                asg['uuid'] = str(uuid.uuid1())
                res.append([asg, loc])
        return res

    def citation_list(self):
        """
        Returns a list of two lists. The first list is normal citations,
        the second is other citations.
        citation:
          date
          name
          kind
          country
          category
          number
          sequence
        OR
        otherreference:
          text
          sequence
        """
        citations = self.xml.us_references_cited.us_citation
        if not citations:
            return [[], []]
        regular_cits = []
        other_cits = []
        ocnt = 0
        ccnt = 0
        for citation in citations:
            data = {}
            if citation.othercit:
                data['text'] = citation.contents_of('othercit', as_string=True, upper=False)
                if any(data.values()):
                    data['sequence'] = ocnt
                    data['uuid'] = str(uuid.uuid1())
                    other_cits.append(data)
                    ocnt += 1
            else:
                for tag in ['name', 'kind', 'category']:
                    data[tag] = citation.contents_of(tag, as_string=True, upper=False)
                data['date'] = self._fix_date(citation.contents_of('date', as_string=True))
                data['country'] = citation.contents_of('country', default=[''])[0]
                doc_number = citation.contents_of('doc_number', as_string=True)
                data['number'] = xml_util.normalize_document_identifier(doc_number)
                if any(data.values()):
                    data['sequence'] = ccnt
                    data['uuid'] = str(uuid.uuid1())
                    regular_cits.append(data)
                    ccnt += 1
        return [regular_cits, other_cits]

    def inventor_list(self):
        """
        Returns list of lists of inventor dictionary and location dictionary
        inventor:
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
        inventors = self.xml.inventors.inventor
        if not inventors:
            return []
        res = []
        for i, inventor in enumerate(inventors):
            # add inventor data
            inv = {}
            inv.update(self._name_helper_dict(inventor.addressbook))
            inv['nationality'] = inventor.nationality.contents_of('country', as_string=True)
            # add location data for inventor
            loc = {}
            for tag in ['city', 'state', 'country']:
                loc[tag] = inventor.addressbook.contents_of(tag, as_string=True, upper=False)
            #this is created because of MySQL foreign key case sensitivities
            loc['id'] = unidecode("|".join([loc['city'], loc['state'], loc['country']]).lower())
            if any(inv.values()) or any(loc.values()):
                inv['sequence'] = i
                inv['uuid'] = str(uuid.uuid1())
                res.append([inv, loc])
        return res

    def lawyer_list(self):
        """
        Returns a list of lawyer dictionary
        lawyer:
            name_last
            name_first
            organization
            country
            sequence
        """
        lawyers = self.xml.agents.agent
        if not lawyers:
            return []
        res = []
        for i, lawyer in enumerate(lawyers):
            law = {}
            law.update(self._name_helper_dict(lawyer))
            law['country'] = lawyer.contents_of('country', as_string=True)
            law['organization'] = lawyer.contents_of('orgname', as_string=True, upper=False)
            law['organization_upper'] = law['organization'].upper()
            if any(law.values()):
                law['uuid'] = str(uuid.uuid1())
                res.append(law)
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
        # TODO: look at PatentGrantXMLv44, page 30 and onward and figure out the best way to parse this
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
