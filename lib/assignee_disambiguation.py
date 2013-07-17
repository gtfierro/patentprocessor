#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
import redis
import uuid
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import fetch_session # gives us the `session` variable
from alchemy.schema import *
from handlers.xml_util import normalize_utf8

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
    """
    Iterates through all assignees. If the strings match within the THRESHOLD confidence,
    we put them into the same block, else put the current assignee in its own block. Blocks
    are stored as redis lists, named by the first ID we encounter for that block
    """
    redis_index = 0
    assignees = list_of_assignees[:]
    for assignee in assignees:
        r.lpush(get_assignee_id(assignee), assignee.uuid)
    sorted_assignees = sorted(map(get_assignee_id, assignees))
    r.lpush(redis_index, sorted_assignees[0])
    for index in xrange(1, len(sorted_assignees)):
        current_assignee = sorted_assignees[index]
        previous_assignee = sorted_assignees[index-1]
        # if the current doesn't match, move to next bucket
        if jaro_winkler(current_assignee, previous_assignee) < THRESHOLD:
            redis_index += 1
        r.lpush(redis_index, current_assignee)
    r.set('num_blocks', redis_index)

def disambiguate_by_frequency(block_number):
    """
    For block, find the most frequent assignee name and create a hash from each
    assignee organization/name to the most frequent name. Delete the old block
    during this process. This ensures that the only keys left in our database
    are the disambiguations.
    """
    assignees = r.lrange(block_number, 0, -1)
    most_common_id = Counter(assignees).most_common()[0][0]
    return most_common_id

def create_assignee_table():
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    for i in xrange(int(r.get('num_blocks'))):
        disambiguated_name = disambiguate_by_frequency(i)
        disambiguated_name = normalize_utf8(disambiguated_name)

        record = {'id': str(uuid.uuid1())} # dict for insertion
        if '|' in disambiguated_name:
            record['name_first'] = disambiguated_name.split('|')[0]
            record['name_last'] = disambiguated_name.split('|')[1]
        else:
            record['organization'] = disambiguated_name
        assignee_obj = Assignee(**record)
        for rawassignee in r.lrange(i, 0, -1):
            for assignee_id in r.lrange(rawassignee, 0, -1):
                ra = assignee_dict[assignee_id]
                assignee_obj.rawassignees.append(ra)
        s.merge(assignee_obj)
    try:
        s.commit()
    except Exception, e:
        s.rollback()


def examine():
    assignees = s.query(Assignee).all()
    print len(assignees)
    print len(s.query(RawAssignee).all())
    print s.query(Assignee).filter_by(organization = 'Cisco Technology, Inc.').first().rawassignees
    print s.query(RawAssignee).first().assignee


if __name__=='__main__':
    create_assignee_blocks(assignees)
    create_assignee_table()
    examine()
