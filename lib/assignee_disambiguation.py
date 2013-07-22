#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
import itertools
from collections import defaultdict
import uuid
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import fetch_session # gives us the `session` variable
from alchemy.schema import *
from handlers.xml_util import normalize_utf8

THRESHOLD = 0.95

# get alchemy.db from the directory above
s = fetch_session(path_to_sqlite='..')

# bookkeeping for blocks
blocks = defaultdict(list)
id_map = defaultdict(list)

# get all assignees in database
assignees = s.query(RawAssignee).all()
assignee_dict = dict(zip([a.uuid for a in assignees], assignees))

def get_assignee_id(obj):
    """
    Returns string representing an assignee object. Returns obj.organization if
    it exists, else returns concatenated obj.name_first + '|' + obj.name_last
    """
    if obj.organization: return obj.organization
    try:
        return obj.name_first + '|' + obj.name_last
    except:
        return ''

def create_assignee_blocks(list_of_assignees):
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
            if jaro_winkler(primary, secondary) >= THRESHOLD:
                assignees.remove(secondary)
                blocks[primary].append(secondary)

def disambiguate_by_frequency(block_key):
    """
    For block, find the most frequent assignee attributes, and return a dict
    of those values.
    """
    assignees = blocks[block_key]
    rawassignees = []
    for rawassignee in assignees:
        rawassignees.extend([assignee_dict[raw_id] for raw_id in id_map[rawassignee]])
    results = {}
    for key in ('type', 'residence', 'nationality'):
        results[key] = Counter(map(lambda x: getattr(x, key),
                                    rawassignees)).most_common()[0][0]
    results['most_common_id'] = Counter(assignees).most_common()[0][0]
    return results

def create_assignee_table():
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    print len(blocks.keys())
    for assignee in blocks.iterkeys():
        disambiguated_dict = disambiguate_by_frequency(assignee)
        disambiguated_name = disambiguated_dict.pop('most_common_id')
        disambiguated_name = normalize_utf8(disambiguated_name)

        record = {'id': str(uuid.uuid1())} # dict for insertion
        record.update(disambiguated_dict)
        if '|' in disambiguated_name:
            record['name_first'] = disambiguated_name.split('|')[0]
            record['name_last'] = disambiguated_name.split('|')[1]
        else:
            record['organization'] = disambiguated_name
        assignee_obj = Assignee(**record)
        for rawassignee in blocks[assignee]:
            for assignee_id in id_map[rawassignee]:
                ra = assignee_dict[assignee_id]
                assignee_obj.rawassignees.append(ra)
        s.merge(assignee_obj)
    try:
        s.commit()
    except Exception, e:
        s.rollback()


def examine():
    assignees = s.query(Assignee).all()
    for a in assignees:
        if len(a.rawassignees) > 1:
            print a, a.rawassignees
            print '-'*10
    print len(assignees)

def run_disambiguation():
    create_assignee_blocks(assignees)
    create_assignee_table()

if __name__=='__main__':
    run_disambiguation()
