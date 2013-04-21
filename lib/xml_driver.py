#!/usr/bin/env python

"""
General purpose XML parsing driver for use as a content handler through
Python's xml.sax module.  Works in conjunction with lib/xml_util.py, which
provides useful helper methods to handle the parsed data.
"""

import re
from itertools import chain, izip
from collections import deque
from xml.sax import make_parser, handler, saxutils
from cgi import escape

class ChainList(list):
    """
    This is the base structure that handles the tree created by XMLElement
    and XMLHandler. Overriding __getattr__ allows us to chain queries on
    a list in order to traverse the tree.
    """

    def contents_of(self, tag, default=['']):
        res = []
        for item in self:
            res.extend( item.contents_of(tag) )
        return ChainList(res) if res else default

    def __getattr__(self, key):
        res = []
        scope = deque(self)
        while scope:
            current = scope.popleft()
            if current._name == key: res.append(current)
            else: scope.extend(current.children)
        return ChainList(res)

class XMLElement(object):
    """
    Represents XML elements from a document. These will assist
    us in representing an XML document as a Python object.
    Heavily inspired from: https://github.com/stchris/untangle/blob/master/untangle.py
    """

    def __init__(self, name, attributes):
        self._name = name
        self._attributes = attributes
        self.content = []
        self.children = ChainList()
        self.is_root = False

    def __iter__(self):
        yield self

    def __nonzero__(self):
        return self.is_root or self._name is not None

    def __getitem__(self, key):
        return self.get_attribute(key)

    def __getattr__(self, key):
        res = []
        scope = deque(self.children)
        while scope:
            current = scope.popleft()
            if current._name == key: res.append(current)
            else: scope.extend(current.children)
        if res:
            self.__dict__[key] = ChainList(res)
            return ChainList(res)
        else:
            return ChainList('')

    def contents_of(self, key, default=ChainList('')):
        candidates = self.__getattr__(key)
        if candidates:
            return [x.get_content() for x in candidates]
        else:
            return default

    def get_content(self):
        if len(self.content) == 1:
            return self.content[0]
        else: return self.content

    def add_child(self, child):
        self.children.append(child)

    def get_attribute(self, key):
        return self._attributes.get(key, None)

    def get_xmlelements(self, name):
        return filter(lambda x: x._name == name, self.children) \
               if name else \
               self.children


class XMLHandler(handler.ContentHandler):
    """
    SAX Handler to create the Python object while parsing
    """

    def __init__(self):
        self.root = XMLElement(None, None)
        self.root.is_root = True
        self.elements = ChainList()

    def startElement(self, name, attributes):
        name = name.replace('-','_').replace('.','_').replace(':','_')
        xmlelem = XMLElement(name, dict(attributes.items()))
        if self.elements:
            self.elements[-1].add_child(xmlelem)
        else:
            self.root.add_child(xmlelem)
        self.elements.append(xmlelem)

    def endElement(self, name):
        if self.elements:
            self.elements.pop()

    def characters(self, content):
        if content.strip():
          if self.elements[-1]._name == 'sub':
            newtxt = u"<sub>"+content+u"</sub>"
            self.elements[-2].content.append(newtxt)
          else:
            self.elements[-1].content.append(saxutils.unescape(content))
