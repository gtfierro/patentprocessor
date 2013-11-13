#!/usr/bin/env python

from lib import assignee_disambiguation
from lib import lawyer_disambiguation
from lib import geoalchemy

def disambiguate(doctype='grant'):
    # run assignee disambiguation and populate the Assignee table
    assignee_disambiguation.run_disambiguation(doctype)

    # run lawyer disambiguation
    if doctype == 'grant':
      lawyer_disambiguation.run_disambiguation()

    #Run new geocoding
    geoalchemy.main(doctype=doctype)

if __name__ == '__main__':
    disambiguate()
