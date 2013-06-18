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

    def _asg_list(self):
        doc = self.xml.assignees.assignee
        data = []
        if not doc: return []
        if doc.first_name:
            data = [1]
            data.extend(doc.contents_of('last_name'))
            data.extend(doc.contents_of('first_name'))
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

    def _inv_list(self):
        inventors = self.xml.parties.applicant
        if not inventors: return []
        res = []
        for inventor in inventors:
            data = []
            lastname = inventor.addressbook.contents_of('last_name',as_string=True)
            firstname = inventor.addressbook.contents_of('first_name',as_string=True)
            firstname, lastname = associate_prefix(firstname, lastname)
            data.append(lastname)
            data.append(firstname)
            for tag in ['street','city','state','country','postcode']:
                data.append(inventor.addressbook.address.contents_of(tag,as_string=True))
            data.append(inventor.nationality.contents_of('country',as_string=True))
            data.append(inventor.residence.contents_of('country',as_string=True))
            res.append(data)
        return res

    def _law_list(self):
        doc = self.xml.parties.agents
        if not doc: return []
        res = []
        for agent in doc.agent:
          tmp = []
          for tag in ['last_name','first_name','country','orgname']:
              data = agent.contents_of(tag)
              tmp.extend([''.join(x) for x in data] if data else [''])
          res.append(tmp)
        return res
