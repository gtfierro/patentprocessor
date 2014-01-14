#!/usr/bin/env Python
"""
Performs a basic assignee disambiguation
"""
from collections import defaultdict, deque
import uuid
from string import lowercase as alphabet
import md5
import cPickle as pickle
import alchemy
from collections import Counter
from Levenshtein import jaro_winkler
from alchemy import get_config, match
from alchemy.schema import *
from alchemy.match import commit_inserts, commit_updates
from handlers.xml_util import normalize_utf8
from datetime import datetime
from sqlalchemy.sql import or_
from sqlalchemy.sql.expression import bindparam
from unidecode import unidecode
from tasks import celery_commit_inserts, celery_commit_updates
import sys
config = get_config()

THRESHOLD = config.get("assignee").get("threshold")

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


def without_digits(word):
    return ''.join([x for x in word if not x.isdigit()])

def create_jw_blocks(list_of_assignees):
    """
    Receives list of blocks, where a block is a list of assignees
    that all begin with the same letter. Within each block, does
    a pairwise jaro winkler comparison to block assignees together
    """
    global blocks
    consumed = defaultdict(int)
    print 'Doing pairwise Jaro-Winkler...'
    for i, primary in enumerate(list_of_assignees):
        if consumed[primary]: continue
        consumed[primary] = 1
        blocks[primary].append(primary)
        for secondary in list_of_assignees[i:]:
            if consumed[secondary]: continue
            if primary == secondary:
                blocks[primary].append(secondary)
                continue
            primary = without_digits(primary)
            secondary = without_digits(secondary)
            if jaro_winkler(primary, secondary, 0.0) >= THRESHOLD:
                consumed[secondary] = 1
                blocks[primary].append(secondary)
    pickle.dump(blocks, open('assignee.pickle', 'wb'))
    print 'Assignee blocks created!'


assignee_insert_statements = []
patentassignee_insert_statements = []
update_statements = []
def create_assignee_table(session):
    """
    Given a list of assignees and the redis key-value disambiguation,
    populates the Assignee table in the database
    """
    print 'Disambiguating assignees...'
    if alchemy.is_mysql():
        session.execute('set foreign_key_checks = 0;')
        session.commit()
    i = 0
    for assignee in blocks.iterkeys():
        ra_ids = (id_map[ra] for ra in blocks[assignee])
        for block in ra_ids:
          i += 1
          rawassignees = [assignee_dict[ra_id] for ra_id in block]
          if i % 20000 == 0:
              print i, datetime.now()
              assignee_match(rawassignees, session, commit=True)
          else:
              assignee_match(rawassignees, session, commit=False)
    t1 = celery_commit_inserts.delay(assignee_insert_statements, Assignee.__table__, alchemy.is_mysql(), 20000)
    t2 = celery_commit_inserts.delay(patentassignee_insert_statements, patentassignee, alchemy.is_mysql(), 20000)
    t3 = celery_commit_updates.delay('assignee_id', update_statements, RawAssignee.__table__, alchemy.is_mysql(), 20000)
    t1.get()
    t2.get()
    t3.get()
    session.commit()
    print i, datetime.now()

def assignee_match(objects, session, commit=False):
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None
    class_type = None
    class_type = None
    for obj in objects:
        if not obj: continue
        class_type = obj.__related__
        raw_objects.append(obj)
        break

    param = {}
    for obj in raw_objects:
        for k, v in obj.summarize.iteritems():
            freq[k][v] += 1
        if "id" not in param:
            param["id"] = obj.uuid
        param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]
    if not param.has_key('organization'):
        param['organization'] = ''
    if not param.has_key('type'):
        param['type'] = ''
    if not param.has_key('name_last'):
        param['name_last'] = ''
    if not param.has_key('name_first'):
        param['name_first'] = ''
    if not param.has_key('residence'):
        param['residence'] = ''
    if not param.has_key('nationality'):
        param['nationality'] = ''

    if param["organization"]:
      param["id"] = md5.md5(unidecode(param["organization"])).hexdigest()
    if param["name_last"]:
      param["id"] = md5.md5(unidecode(param["name_last"]+param["name_first"])).hexdigest()
    
    assignee_insert_statements.append(param)
    tmpids = map(lambda x: x.uuid, objects)
    patents = map(lambda x: x.patent_id, objects)
    patentassignee_insert_statements.extend([{'patent_id': x, 'assignee_id': param['id']} for x in patents])
    update_statements.extend([{'pk':x,'update':param['id']} for x in tmpids])

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
    create_assignee_table(session)


def run_disambiguation(doctype='grant'):
    # get all assignees in database
    global blocks
    global assignee_insert_statements
    global patentassignee_insert_statements
    global update_statements
    session = alchemy.fetch_session(dbtype=doctype)
    if doctype == 'grant':
        assignees = deque(session.query(RawAssignee))
    if doctype == 'application':
        assignees = deque(session.query(App_RawAssignee))
    assignee_alpha_blocks = clean_assignees(assignees)
    for letter in alphabet:
        blocks = defaultdict(list)
        assignee_insert_statements = []
        patentassignee_insert_statements = []
        update_statements = []
        letterblock = [x for x in assignee_alpha_blocks if x.lower().startswith(letter)]
        create_jw_blocks(letterblock)
        create_assignee_table(session)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Need doctype"
        sys.exit(0)
    elif len(sys.argv) < 3:
        doctype = sys.argv[1]
        print ('Running ' + doctype)
        run_disambiguation(doctype)
    else:
        doctype = sys.argv[1]
        letter = sys.argv[2]
        session = alchemy.fetch_session(dbtype=doctype)
        print('Running ' + letter + ' ' + doctype)
        run_letter(letter, session, doctype)
