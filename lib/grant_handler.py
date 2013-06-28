#!/usr/bin/env python

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

import cStringIO
from xml_driver import *
from xml_util import *
from xml.sax import xmlreader

class PatentGrant(object):

    def __init__(self, filename, is_string=False):
        xh = XMLHandler()
        parser = make_parser()
        parser.setContentHandler(xh)
        parser.setFeature(handler.feature_external_ges, False)
        l = xmlreader.Locator()
        xh.setDocumentLocator(l)
        if is_string:
            parser.parse(cStringIO.StringIO(filename))
        else:
            parser.parse(filename)
        self.xml = xh.root.us_patent_grant.us_bibliographic_data_grant

        self.country = self.xml.publication_reference.contents_of('country')[0]
        self.patent = normalize_document_identifier(self.xml.publication_reference.contents_of('doc_number')[0])
        self.kind = self.xml.publication_reference.contents_of('kind')[0]
        self.date_grant = self.xml.publication_reference.contents_of('date')[0]
        self.pat_type = self.xml.application_reference[0].get_attribute('appl-type')
        self.date_app = self.xml.application_reference.contents_of('date')[0]
        self.country_app = self.xml.application_reference.contents_of('country')[0]
        self.patent_app = self.xml.application_reference.contents_of('doc_number')[0]
        self.code_app = self.xml.contents_of('us_application_series_code')[0]
        self.clm_num = self.xml.contents_of('number_of_claims')[0]
        self.classes = self._classes()
        self.abstract = self.xml.contents_of('abstract','')
        self.invention_title = self._invention_title()
        self.asg_list = self._asg_list()
        self.cit_list = self._cit_list()
        self.rel_list = self._rel_list()
        self.inv_list = self._inv_list()
        self.law_list = self._law_list()

    def _invention_title(self):
        original = self.xml.contents_of('invention_title')[0]
        if isinstance(original, list):
          original = ''.join(original)
        return original

    def _classes(self):
        main = self.xml.classification_national.contents_of('main_classification')
        further = self.xml.classification_national.contents_of('further_classification')
        it = [main[0] if has_content(main) else []]
        if has_content(further):
            it.extend(further)
        return [ [x[:3].replace(' ',''), x[3:].replace(' ','')] for x in it]

    def _name_helper(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True)
        lastname = tag_root.contents_of('last_name', as_string=True)
        return associate_prefix(firstname, lastname)

    def _name_helper_dict(self, tag_root):
        """
        Returns dictionary of firstname, lastname with prefix associated
        with lastname
        """
        firstname = tag_root.contents_of('first_name', as_string=True)
        lastname = tag_root.contents_of('last_name', as_string=True)
        firstname, lastname = associate_prefix(firstname, lastname)
        return {'name_first':firstname, 'name_last':lastname}

    def _add_sequence(self, list_of_fields):
        """
        Given a list of lists (the internal lists will represent lawyers, etc), extend
        each list by the order it is received. The first list will have sequence 0,
        the second will have sequence 1, etc
        """
        res = []
        for idx, item in enumerate(list_of_fields):
            item.append(idx)
            res.append(item)
        return res

    def _asg_list(self):
        doc = self.xml.assignees.assignee
        data = []
        if not doc: return []
        if doc.first_name:
            data = [1]
            firstname, lastname = self._name_helper(doc)
            data.extend(lastname)
            data.extend(firstname)
        else:
            data = [0]
            data.extend(doc.contents_of('orgname'))
            data.extend(doc.contents_of('role'))
        for tag in ['street','city','state','country','postcode']:
            data.extend(doc.addressbook.address.contents_of(tag))
        data.extend(doc.nationality.contents_of('country'))
        data.extend(doc.residence.contents_of('country'))
        return [data]

    def _cit_list(self):
        res = []
        citations = self.xml.references_cited.citation
        for citation in citations:
            cit_data = citation.contents_of('category')
            if citation.patcit:
                for tag in ['country','doc_number','date','kind','name']:
                    contents = citation.contents_of(tag)
                    if isinstance(contents, list) and contents:
                        cit_data.append(contents[0])
                    else:
                        cit_data.append(contents if has_content(contents) else '')
                cit_data.append('')
            if citation.othercit:
                contents = citation.contents_of('othercit')
                for chunk in contents:
                    cit_data.extend(['','','','',''])
                    if isinstance(chunk,list):
                        cit_data.append(''.join([escape_html_nosub(x) for x in chunk]).upper())
                    else:
                        cit_data.append(escape_html_nosub(chunk))
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
                contents = root.contents_of(tag,default=[''])
                data.extend(contents[:1] if isinstance(contents,list) else [contents])
            res.append(data)
        return res

    def _rel_list(self):
        res = []
        taglist = ['doc_number','country','kind']
        for tag in ['continuation_in_part','continuation','division','reissue']:
            main = self.xml.__getattr__(tag)
            if not main:
                continue
            tag = tag.replace('_','-').upper()
            relations = main.relation # get all relations
            for relation in relations:
                if relation.child_doc:
                    res.extend(self._rel_helper([tag, -1], relation.child_doc, taglist))
                base = [tag, 1]
                taglist.extend(['date','parent_status'])
                if relation.parent_doc:
                    res.extend(self._rel_helper(base, relation.parent_doc, taglist))
                if relation.parent_doc.parent_grant_document:
                    res.extend(self._rel_helper(base, relation.parent_doc.parent_grant_document, taglist))
                if relation.parent_doc.parent_pct_document:
                    res.extend(self._rel_helper(base, relation.parent_doc.parent_pct_document, taglist))
            if res: break
        for tag in ['related-publication','us-provisional-application']:
            if not self.xml.__getattr__(tag):
                continue
            if self.xml.document_id:
                tmp = [tag, 0]
                for nested in ['doc_number','country','kind']:
                    tmp.extend(self.xml.document_id.contents_of(nested))
                res.append(tmp)
            if res: break
        return res

    def augmented_rel_list(self):
        """includes the sequence number at the end of the list"""
        res = []
        seq = []
        root = self.xml.us_related_documents
        if not root: return
        root = root[0]
        for usreldoc in root.children:
            for relation in usreldoc.relation:
                relnumber = -1 if relation.child_doc else 1
                docinfo = [usreldoc._name, relnumber, relation.contents_of('doc_number')[0],\
                          relation.contents_of('country')[0], relation.contents_of('kind')]
                print docinfo


    def _inv_list(self):
        inventors = self.xml.parties.applicant
        if not inventors: return []
        res = []
        for inventor in inventors:
            data = []
            firstname, lastname = self._name_helper(inventor.addressbook)
            data.append(lastname)
            data.append(firstname)
            for tag in ['street','city','state','country','postcode']:
                data.append(inventor.addressbook.address.contents_of(tag,as_string=True))
            data.append(inventor.nationality.contents_of('country',as_string=True))
            data.append(inventor.residence.contents_of('country',as_string=True))
            res.append(data)
        return res

    def _law_list(self):
        lawyers = self.xml.parties.agents.agent
        if not lawyers: return []
        res = []
        for lawyer in lawyers:
            data = []
            firstname, lastname = self._name_helper(lawyer)
            data.append(lastname)
            data.append(firstname)
            data.append(lawyer.contents_of('country',as_string=True))
            data.append(lawyer.contents_of('orgname',as_string=True))
            res.append(data)
        return res

    def assignee_list(self):
        """
        Returns list of dictionaries:
        assignee:
          name_last
          name_first
          residence
          nationality
          sequence
        location:
          city
          state
          country
        """
        assignees = self.xml.assignees.assignee
        if not assignees: return []
        res = []
        for i,assignee in enumerate(assignees):
            # add assignee data
            asg = {}
            asg.update(self._name_helper_dict(assignee)) # add firstname, lastname
            asg['orgname'] = assignee.contents_of('orgname',as_string=True)
            asg['role'] = assignee.contents_of('role',as_string=True)
            asg['nationality'] = assignee.nationality.contents_of('country')[0]
            asg['residence'] = assignee.nationality.contents_of('country')[0]
            asg['sequence'] = i
            # add location data for assignee
            loc = {}
            for tag in ['city','state','country']:
                loc[tag] = assignee.contents_of(tag,as_string=True)
            res.append([asg, loc])
        return res

    def citation_list(self):
        """
        Returns list of dictionaries:
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
        citations = self.xml.references_cited.citation
        if not citations: return []
        res = []
        for i,citation in enumerate(citations):
            data = {}
            for tag in ['date','name','kind','category']:
                data[tag] = citation.contents_of(tag, as_string=True)
            data['country'] = citation.contents_of('country', default=[''])[0]
            doc_number = citation.contents_of('doc_number', as_string=True)
            data['number'] = normalize_document_identifier(doc_number)
            data['text'] = citation.contents_of('othercit', as_string=True)
            data['sequence'] = i
            res.append(data)
        return res

    def inventor_list(self):
        """
        Returns list of lists of inventor dictionary and location dictionary
        inventor:
          name_last
          name_first
          nationality
          sequence
        location:
          city
          state
          country
        """
        inventors = self.xml.parties.applicant
        if not inventors: return []
        res = []
        for i,inventor in enumerate(inventors):
            # add inventor data
            inv = {}
            inv.update(self._name_helper_dict(inventor.addressbook))
            inv['nationality'] = inventor.nationality.contents_of('country', as_string=True)
            inv['sequence'] = i
            # add location data for inventor
            loc = {}
            for tag in ['city','state','country']:
                loc[tag] = inventor.addressbook.contents_of(tag,as_string=True)
            res.append([inv, loc])
        return res

    def lawyer_list(self):
        lawyers = self.xml.parties.agents.agent
        if not lawyers: return []
        res = []
        for i,lawyer in enumerate(lawyers):
            law = {}
            law.update(self._name_helper_dict(lawyer))
            law['country'] = lawyer.contents_of('country',as_string=True)
            law['orgname'] = lawyer.contents_of('orgname',as_string=True)
            res.append(law)
        return res

