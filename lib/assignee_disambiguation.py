#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
from collections import defaultdict, deque
import uuid
import cPickle as pickle
import alchemy
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import get_config, match
from alchemy.schema import *
from handlers.xml_util import normalize_utf8
from datetime import datetime
from sqlalchemy.sql import or_
from sqlalchemy.sql.expression import bindparam
import sys
config = get_config()

THRESHOLD = config.get("assignee").get("threshold")

def get_assignee_id(obj):
    """
    Returns string representing an assignee object. Returns obj.organization if
    it exists, else returns concatenated obj.name_first + '|' + obj.name_last
    """
    if obj.organization:
        return obj.organization
    try:
        return obj.name_first + '|' + obj.name_last
    except:
        return ''

def clean_assignees(list_of_assignees, id_map, assignee_dict):
    """
    Removes the following stop words from each assignee:
    the, of, and, a, an, at
    Then, blocks the assignee with other assignees that start
    with the same letter. Returns a list of these blocks
    """
    stoplist = ['the', 'of', 'and', 'a', 'an', 'at']
    #alpha_blocks = defaultdict(list)
    block = []
    print 'Removing stop words, blocking by first letter...'
    for assignee in list_of_assignees:
        assignee_dict[assignee.uuid] = assignee
        a_id = get_assignee_id(assignee)
        # removes stop words, then rejoins the string
        a_id = ' '.join(filter(lambda x:
                            x.lower() not in stoplist,
                            a_id.split(' ')))
        id_map[a_id].append(assignee.uuid)
        block.append(a_id)
    print 'Assignees cleaned!'
    return block


def create_jw_blocks(list_of_assignees, blocks):
    """
    Receives list of blocks, where a block is a list of assignees
    that all begin with the same letter. Within each block, does
    a pairwise jaro winkler comparison to block assignees together
    """
    consumed = defaultdict(int)
    print 'Doing pairwise Jaro-Winkler...'
    for primary in list_of_assignees:
        if consumed[primary]: continue
        consumed[primary] = 1
        blocks[primary].append(primary)
        for secondary in list_of_assignees:
            if consumed[secondary]: continue
            if primary == secondary:
                blocks[primary].append(secondary)
                continue
            if jaro_winkler(primary, secondary, 0.0) >= THRESHOLD:
                consumed[secondary] = 1
                blocks[primary].append(secondary)
    pickle.dump(blocks, open('assignee.pickle', 'wb'))
    print 'Assignee blocks created!'


def create_assignee_table(session, blocks, id_map, assignee_dict):
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    print 'Disambiguating assignees...'
    i = 0
    for assignee in blocks.iterkeys():
        ra_ids = (id_map[ra] for ra in blocks[assignee])
        for block in ra_ids:
          i += 1
          rawassignees = [assignee_dict[ra_id] for ra_id in block]
          if i % 10000 == 0:
              print i, datetime.now()
              match(rawassignees, session, commit=True)
          else:
              match(rawassignees, session, commit=False)
    session.commit()
    print i, datetime.now()

def examine():
    assignees = s.query(Assignee).all()
    for a in assignees:
        print get_assignee_id(a), len(a.rawassignees)
        for ra in a.rawassignees:
            if get_assignee_id(ra) != get_assignee_id(a):
                print get_assignee_id(ra)
            print '-'*10
    print len(assignees)


def printall():
    assignees = s.query(Assignee).all()
    with open('out.txt', 'wb') as f:
        for a in assignees:
            f.write(normalize_utf8(get_assignee_id(a)).encode('utf-8'))
            f.write('\n')
            for ra in a.rawassignees:
                f.write(normalize_utf8(get_assignee_id(ra)).encode('utf-8'))
                f.write('\n')
            f.write('-'*20)
            f.write('\n')

def run_letter(letter, session, doctype='grant'):
    schema = RawAssignee
    if doctype == 'application':
        schema = App_RawAssignee
    letter = letter.upper()
    clause1 = schema.organization.startswith(bindparam('letter',letter))
    clause2 = schema.name_first.startswith(bindparam('letter',letter))
    clauses = or_(clause1, clause2)
    assignees = (assignee for assignee in session.query(schema).filter(clauses))
    block = clean_assignees(assignees)
    create_jw_blocks(block)
    create_assignee_table()

def run_disambiguation(doctype='grant'):
    blocks = defaultdict(list)
    id_map = defaultdict(list)
    assignee_dict = {}

    # get all assignees in database
    session = alchemy.fetch_session(dbtype=doctype)
    if doctype == 'grant':
        assignees = deque(session.query(RawAssignee))
    if doctype == 'application':
        assignees = deque(session.query(App_RawAssignee))
    assignee_alpha_blocks = clean_assignees(assignees, id_map, assignee_dict)
    create_jw_blocks(assignee_alpha_blocks, blocks)
    create_assignee_table(session, blocks, id_map, assignee_dict)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        run_disambiguation()
    elif len(sys.argv) < 3:
        doctype = sys.argv[1]
        print ('Running ' + doctype)
        run_disambiguation(doctype)
    else:
        doctype = sys.argv[1]
        letter = sys.argv[1]
        session = alchemy.fetch_session(dbtype=doctype)
        print('Running ' + letter + ' ' + doctype)
        run_letter(letter, session, doctype)
