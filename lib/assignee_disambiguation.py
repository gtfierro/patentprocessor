#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
import redis
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import fetch_session # gives us the `session` variable
from alchemy.schema import *

THRESHOLD = 0.95

# get alchemy.db from the directory above
s = fetch_session(path_to_sqlite='..')

# get redis session
r = redis.StrictRedis(host='localhost')

# delete all previous keys
for i in r.keys():
    r.delete(i)

# get all assignees in database
assignees = s.query(RawAssignee).all()

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

def create_assignee_blocks(assignees):
    """
    Iterates through all assignees. If the strings match within the THRESHOLD confidence,
    we put them into the same block, else put the current assignee in its own block. Blocks
    are stored as redis lists, named by the first ID we encounter for that block
    """
    for current in assignees:
        for assignee in assignees:
            if current == assignee: continue
            current_id = get_assignee_id(current)
            assignee_id = get_assignee_id(assignee)
            if jaro_winkler(current_id, assignee_id) >= THRESHOLD:
                # block name is the first id we encountered
                r.lpush(current_id, assignee_id, current_id)
                assignees.remove(assignee)
                assignees.remove(current)
            else:
                r.lpush(current_id, current_id)
                #assignees.remove(current)

def disambiguate_by_frequency():
    """
    For block, find the most frequent assignee name and create a hash from each
    assignee organization/name to the most frequent name. Delete the old block
    during this process. This ensures that the only keys left in our database
    are the disambiguations.
    """
    for block in r.keys():
        assignees = r.lrange(block, 0, -1) # get all elements in list [block]
        most_common_id = Counter(assignees).most_common()[0][0]
        r.delete(block)
        for assignee in assignees:
            r.set(assignee, most_common_id)

def create_assignee_table(assignees):
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    for assignee in assignees:
        record = {} # dict for insertion
        disambiguated_name = r.get(get_assignee_id(assignee))
        if assignee.organization:
            record['organization'] = disambiguated_name
        else:
            record['name_first'] = disambiguated_name.split('|')[0]
            record['name_last'] = disambiguated_name.split('|')[1]
        for key in ['residence', 'nationality', 'type']:
            record[key] = getattr(assignee, key)
        record['id'] = assignee.uuid
        assignee_obj = Assignee(**record)
        assignee_obj.rawassignees.append(assignee)
        s.merge(assignee_obj)
    try:
        s.commit()
    except Exception, e:
        s.rollback()


def examine():
    assignees = s.query(Assignee).all()
    for a in assignees:
        print a.id, a.rawassignees


if __name__=='__main__':
    create_assignee_blocks(assignees)
    disambiguate_by_frequency()
    create_assignee_table(assignees)
