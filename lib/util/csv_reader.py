#!/usr/bin/env python

"""
Simplifies the process for reading in unicode CSV files
"""

import csv
from unicodedata import normalize
import codecs

def unicode_csv_reader(unicode_csv_data, dialect=csv.excel, **kwargs):
    """
    Creates a unicode CSV reader
    """
    csv_reader = csv.reader(utf_8_encoder(unicode_csv_data), dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]

def utf_8_encoder(unicode_csv_data):
    """
    Encodes data in utf-8
    """
    for line in unicode_csv_data:
        yield line.encode('utf-8')

def read_file(filename):
    """
    Given a string [filename], returns an iterator of the lines in the CSV file
    """
    with codecs.open(filename, encoding='utf-8') as csvfile:
        reader = unicode_csv_reader(csvfile)
        for row in reader:
            yield row
