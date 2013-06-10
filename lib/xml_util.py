#!/usr/bin/env python

"""
Collection of useful functions and tools for working with XML documents
"""

import re
from itertools import chain, izip
from unicodedata import normalize
from cgi import escape

def flatten(ls_of_ls):
    """
    Takes in a list of lists, returns a new list of lists
    where list `i` contains the `i`th element from the original
    set of lists.
    """
    return map(list, list(izip(*ls_of_ls)))

def extend_padding(ls_of_ls, padding=''):
    """
    Takes in a lists of lists, returns a new list of lists
    where each list is padded up to the length of the longest
    list by [padding] (defaults to the empty string)
    """
    maxlen = max(map(len, ls_of_ls))
    newls = []
    for ls in ls_of_ls:
        if len(ls) != maxlen:
            ls.extend([padding]*(maxlen - len(ls)))
        newls.append(ls)
    return newls

def escape_html_nosub(string):
    """
    Escapes html sequences (e.g. <b></b>) that are not the known idiom
    for subscript: <sub>...</sub>
    """
    lt = re.compile('<(?!/?sub>)')
    gt = re.compile('(?=.)*(?<!sub)>')
    string = string.replace('&','&amp;')
    string = re.sub(lt,"&lt;",string)
    string = re.sub(gt,"&gt;",string)
    return string

def has_content(l):
    """
    Returns true if list [l] contains any non-null objects
    """
    return any(filter(lambda x: x, l))

def normalize_utf8(string):
    """
    Normalizes [string] to be UTF-8 encoded. Accepts both unicode and normal
    Python strings.
    """
    if isinstance(string, unicode):
        return normalize('NFC', string)
    else:
        return normalize('NFC', string.decode('utf-8'))

def remove_escape_sequences(string):
    """
    Replaces all contiguous instances of "\r\n\t\v\b\f\a " and replaces
    it with a single space. Preserves at most one space of surrounding whitespace
    """
    escape_seqs = r'[\r\n\t\v\b\f\a ]+'
    return re.sub(escape_seqs,' ', string)

def translate_underscore(string):
    """
    Replaces the underscore HTML idiom <sub>&#x2014;</sub> with the literal
    underscore character _.
    """
    return string.lower().replace('<sub>&#x2014;</sub>','_').replace('<sub>-</sub>','_').replace(u'<sub>\u2014</sub>','_')

def escape_html(string):
    """
    Call cgi.escape on the string after applying translate_underscore
    """
    s = translate_underscore(string)
    return escape(s)

def normalize_document_identifier(identifier):
    """
    [identifier] is a string representing the document-id field from an XML document
    """
    # create splits on identifier
    s = ''
    if identifier.upper().startswith('USD'):
        s = identifier[:3],identifier[3:]
    if identifier.upper().startswith('US'):
        s = identifier[:2],identifier[2:]
    else:
        return identifier
    # strip leading 0 if it exists
    if s[1].startswith('0'):
        s[1] = s[1][1:]
    return s[0]+s[1]

def associate_prefix(firstname, lastname):
    """
    Prepends everything after the first space-delineated word in [firstname] to
    [lastname].
    """
    if ' ' in firstname:
        name, prefix = firstname.split(' ',1) # split on first space
    else:
        name, prefix = firstname, ''
    space = ' '*(prefix is not '')
    last = prefix+space+lastname
    return name, last

def clean(string):
    """
    Applies a subset of the above functions in the correct order
    and returns the string in all uppercase.
    """
    string = normalize_utf8(string)
    string = remove_escape_sequences(string)
    string = translate_underscore(string)
    string = escape_html(string)
    return string.upper()
