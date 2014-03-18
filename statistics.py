#!/usr/bin/env python
"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Takes the database from the output of integrate.py (e.g. after the disambiguated
inventors have been merged into the database) and computes statistics on top of it
"""

import uuid
from datetime import datetime
from lib import alchemy
from collections import Counter, defaultdict

def compute_future_citation_rank():
    """
    Ranks each patent by number of future citations in a given year
    Returns nested dictionary:
        years[YEAR][PATENT_ID] = list of Citation.number that cited PATENT_ID in YEAR
    """
    citations = (c for c in alchemy.session.query(alchemy.grant.USPatentCitation).yield_per(1))
    years = defaultdict(lambda: defaultdict(list))
    print "Counting citations...", datetime.now()
    for cit in citations:
      if cit.date:
        year = cit.date.year
        patid = cit.patent_id
        years[year][patid].append(cit.number)
    print "Finished counting citations", datetime.now()
    return years

def insert_future_citation_rank(years):
    """
    Accepts as input the dictionary returned from compute_future_citation_rank:
        years[YEAR][PATENT_ID] = number of times PATENT_ID was cited in YEAR
    Inserts rows into the correct table:
    """
    # remove old rows to make way for new rankings
    deleted = alchemy.session.query(alchemy.grant.FutureCitationRank).delete()
    print 'Removed {0} rows from FutureCitationRank'.format(deleted)
    print 'Inserting records in order...', datetime.now()
    for year in years.iterkeys():
        rank = 0
        prev_num_cits = float('inf')
        commit_counter = 0
        for i, record in enumerate(years[year].iteritems()):
            if record[1] < prev_num_cits:
                prev_num_cits = len(record[1])
                rank += 1
            row = {'uuid': str(uuid.uuid1()),
                   'patent_id': record[0],
                   'num_citations': len(record[1]),
                   'year': year,
                   'rank': rank}
            dbrow = alchemy.grant.FutureCitationRank(**row)
            alchemy.session.merge(dbrow)
            if (i+1) % 1000 == 0:
                alchemy.commit()
    alchemy.commit()
    print 'Finished inserting records', datetime.now()

def insert_cited_by(years):
    """
    Accepts as input the dictionary returned from compute_future_citation_rank:
        years[YEAR][PATENT_ID] = number of times PATENT_ID was cited in YEAR
    Inserts records into CitedBy table
    """
    deleted = alchemy.session.query(alchemy.grant.CitedBy).delete()
    print 'Removed {0} rows from FutureCitationRank'.format(deleted)
    print 'Inserting records in order...', datetime.now()
    for year in years.iterkeys():
        for i, record in enumerate(years[year].iteritems()):
            row = {'uuid': str(uuid.uuid1()),
                   'patent_id': record[0],
                   'year': year}
            for citation in record[1]:
                row.update({'citation_id': citation})
                dbrow = alchemy.grant.CitedBy(**row)
                alchemy.session.merge(dbrow)
            if (i+1) % 1000 == 0:
                alchemy.commit()
    alchemy.commit()
    print 'Finished inserting records', datetime.now()



def compute_inventor_rank():
    """
    Ranks each inventor by number of granted patents in a given year
    Returns nested dictionary:
        years[YEAR][INVENTOR_ID] = number of patents granted in YEAR to INVENTOR_ID
    """
    patents = (p for p in alchemy.session.query(alchemy.grant.Patent).yield_per(1))
    years = defaultdict(Counter)
    print 'Counting granted patents...', datetime.now()
    for pat in patents:
        year = pat.date.year
        inventors = pat.inventors
        for inventor in inventors:
            years[year][inventor.id] += 1
    print 'Finished counting', datetime.now()
    return years

def insert_inventor_rank(years):
    """
    Accepts as input the dictionary returned from compute_inventor_rank:
        years[YEAR][INVENTOR_ID] = number of patents granted in YEAR to INVENTOR_ID
    Inserts rows into the correct table:
    """
    deleted = alchemy.session.query(alchemy.grant.InventorRank).delete()
    print 'removed {0} rows'.format(deleted)
    print 'Inserting records in order...', datetime.now()
    for year in years.iterkeys():
        rank = 0
        prev_num_cits = float('inf')
        commit_counter = 0
        for i, record in enumerate(years[year].most_common()):
            if record[1] < prev_num_cits:
                prev_num_cits = record[1]
                rank += 1
            row = {'uuid': str(uuid.uuid1()),
                   'inventor_id': record[0],
                   'num_patents': record[1],
                   'year': year,
                   'rank': rank}
            dbrow = alchemy.grant.InventorRank(**row)
            alchemy.session.merge(dbrow)
            if (i+1) % 1000 == 0:
                alchemy.commit()
    alchemy.commit()
    print 'Finished inserting records', datetime.now()


if __name__=='__main__':
    years = compute_future_citation_rank()
    insert_future_citation_rank(years)
    insert_cited_by(years)

    years = compute_inventor_rank()
    insert_inventor_rank(years)
