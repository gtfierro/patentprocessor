#!/usr/bin/env python

"""
Uses the extended ContentHandler from xml_driver to extract the needed fields
from patent grant documents
"""

import cStringIO
from xml_driver import *
from xml_util import *

class PatentGrant(object):

  def __init__(self, filename, is_string=False):
      xh = XMLHandler()
      parser = make_parser()
      parser.setContentHandler(xh)
      parser.setFeature(handler.feature_external_ges, False)
      if is_string:
        parser.parse(cStringIO.StringIO(filename))
      else:
        parser.parse(filename)
      self.xml = xh.root.us_patent_grant.us_bibliographic_data_grant

      self.country = self.xml.publication_reference.contents_of('country')[0]
      self.patent = self.xml.publication_reference.contents_of('doc_number')[0]
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
      self.invention_title = self.xml.contents_of('invention_title')[0]
      self.asg_list = self._asg_list()
      self.cit_list = self._cit_list()
      self.rel_list = self._rel_list()
      self.inv_list = self._inv_list()
      self.law_list = self._law_list()

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

  #TODO: fix text encodings 
  def _cit_list(self):
      res = []
      cits = self.xml.references_cited.citation
      record = cits.contents_of('category')
      res.append(record)
      if cits.patcit:
          for tag in ['country','doc_number','date','kind','name']:
              res.append(cits.patcit.contents_of(tag))
          res[0].extend(extend_padding(res[1:]))
          res.append( [''] * max(map(len, res)))
      contacts = flatten(res)
      last_records = record[len(contacts):]
      if cits.othercit:
          for rec,cit in zip(last_records,cits.contents_of('othercit')):
              tmp = [rec, '', '', '', '' ,'']
              s = ''.join([escape_html_nosub(x) for x in cit])
              tmp.append(s)
              contacts.append(tmp)
      return contacts

  def _rel_list(self):
      res = []
      for tag in ['continuation_in_part','continuation','division','reissue']:
          if not self.xml.__getattr__(tag):
              continue
          tag = tag.replace('_','-')
          if self.xml.relation.child_doc:
              tmp = [tag, -1]
              for nested in ['doc_number','country','kind']:
                  tmp.extend(self.xml.relation.child_doc.contents_of(nested))
              res.append(tmp)
          if self.xml.relation.parent_doc:
              tmp = [tag, 1]
              for nested in ['doc_number','country','kind','date','parent_status']:
                  data = self.xml.relation.parent_doc.contents_of(nested)
                  tmp.append(data[0] if isinstance(data, list) else data)
              res.append(tmp)
          if self.xml.relation.parent_doc.parent_grant_document:
              tmp = [tag, 1]
              for nested in ['doc_number','country','kind','date','parent_status']:
                  tmp.extend(self.xml.relation.parent_grant_document.contents_of(nested))
              res.append(tmp)
          if self.xml.relation.parent_doc.parent_pct_document:
              tmp = [tag, 1]
              for nested in ['doc_number','country','kind','date','parent_status']:
                  tmp.extend(self.xml.relation.parent_pct_document.contents_of(nested))
              res.append(tmp)
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
      doc = self.xml.parties.applicant
      if not doc: return []
      res = []
      res.append(doc.addressbook.contents_of('last_name'))
      res.append(doc.addressbook.contents_of('first_name'))
      for tag in ['street','city','state','country','postcode']:
          data = doc.addressbook.address.contents_of(tag)
          if any(map(lambda x: isinstance(x, list), data)):
              data = [''.join(x) for x in data]
          res.append(data)
      res.append(doc.nationality.contents_of('country'))
      res.append(doc.residence.contents_of('country'))
      maxlen = max(map(len, res))
      res = [x*maxlen if len(x) != maxlen else x for x in res]
      return flatten(res)

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
