#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
from collections import defaultdict
import uuid
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

# get all assignees in database
assignees = [a for a in session.query(RawAssignee).yield_per(1)]
assignee_dict = dict(zip([a.uuid for a in assignees], assignees))


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


def create_assignee_blocks(list_of_assignees):
    print 'Creating assignee blocks...'
    assignees = []
    for assignee in list_of_assignees:
        a_id = get_assignee_id(assignee)
        id_map[a_id].append(assignee.uuid)
        assignees.append(a_id)
    num_blocks = 0
    for primary in assignees:
        assignees.remove(primary)
        blocks[primary].append(primary)
        for secondary in assignees:
            if jaro_winkler(primary, secondary, 0.0) >= THRESHOLD:
                assignees.remove(secondary)
                blocks[primary].append(secondary)
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
          match(rawassignees)

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
    create_assignee_blocks(assignees)
    create_assignee_table()


if __name__ == '__main__':
    run_disambiguation()
