#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
from collections import defaultdict, deque
import uuid
import cPickle as pickle
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import session, get_config, match
from alchemy.schema import *
from handlers.xml_util import normalize_utf8

config = get_config()

THRESHOLD = config.get("assignee").get("threshold")

# get alchemy.db from the directory above

# bookkeeping for blocks
blocks = defaultdict(list)
id_map = defaultdict(list)

assignee_dict = {}


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

def clean_assignees(list_of_assignees):
    """
    Removes the following stop words from each assignee:
    the, of, and, a, an, at
    Then, blocks the assignee with other assignees that start
    with the same letter. Returns a list of these blocks
    """
    stoplist = ['the', 'of', 'and', 'a', 'an', 'at']
    alpha_blocks = defaultdict(list)
    print 'Removing stop words, blocking by first letter...'
    for assignee in list_of_assignees:
        assignee_dict[assignee.uuid] = assignee
        a_id = get_assignee_id(assignee)
        # removes stop words, then rejoins the string
        a_id = ' '.join(filter(lambda x:
                            x.lower() not in stoplist,
                            a_id.split(' ')))
        id_map[a_id].append(assignee.uuid)
        alpha_blocks[a_id[0]].append(a_id)
    print 'Alpha blocks created!'
    return alpha_blocks.itervalues()


def create_jw_blocks(list_of_assignees):
    """
    Receives list of blocks, where a block is a list of assignees
    that all begin with the same letter. Within each block, does
    a pairwise jaro winkler comparison to block assignees together
    """
    consumed = defaultdict(int)
    print 'Doing pairwise Jaro-Winkler...'
    for alphablock in list_of_assignees:
        for primary in alphablock:
            if consumed[primary]: continue
            consumed[primary] = 1
            blocks[primary].append(primary)
            for secondary in alphablock:
                if consumed[secondary]: continue
                if primary == secondary:
                    blocks[primary].append(secondary)
                    continue
                if jaro_winkler(primary, secondary, 0.0) >= THRESHOLD:
                    consumed[secondary] = 1
                    blocks[primary].append(secondary)
    pickle.dump(blocks, open('assignee.pickle', 'wb'))
    print 'Assignee blocks created!'


def create_assignee_table():
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    print 'Disambiguating assignees...'
    for assignee in blocks.iterkeys():
        ra_ids = (id_map[ra] for ra in blocks[assignee])
        for block in ra_ids:
          rawassignees = [assignee_dict[ra_id] for ra_id in block]
          match(rawassignees, session)

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


def run_disambiguation():
    # get all assignees in database
    assignees = deque(session.query(RawAssignee))
    assignee_alpha_blocks = clean_assignees(assignees)
    create_jw_blocks(assignee_alpha_blocks)
    create_assignee_table()


if __name__ == '__main__':
    run_disambiguation()
