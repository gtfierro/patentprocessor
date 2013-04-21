#!/usr/bin/env python

"""
Collection of useful functions and tools for working with XML documents
"""

import re
from itertools import chain, izip

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

def has_content(self, l):
    """
    Returns true if list [l] contains any non-null objects
    """
    return any(filter(lambda x: x, l))
