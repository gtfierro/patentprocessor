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
from collections import defaultdict
from collections import Counter
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import create_engine, MetaData, Table, inspect, VARCHAR, Column
from sqlalchemy.orm import sessionmaker

from datetime import datetime

def match(objects, session, default={}, keepexisting=False, commit=True):
    """
    Pass in several objects and make them equal

    Args:
        objects: A list of RawObjects like RawAssignee
          also supports CleanObjects like Assignee
        keepexisting: Keep the default keyword
        default: Fields to default the clean variable with
        commit: specifies whether we should commit each
          new match call. turning commit off and committing
          at a frequency is useful for bulk updates

        commit: if True, commits the matched objects when
            `match` is called.

            if False, does not commit the matched objects,
            keeping them in an intermediary state within the
            session object. Useful for bulk updates
            (e.g. `commit` the session after every X calls of `match`)

        Default key priority:
        auto > keepexisting > default

        --- code to add later ---
        meant to improve the detection of an item

        raw_objects.append(obj)

        # TODO
        # this function does create some slow down
        # there seems to be something wrong here!
        # if class_type and default:
        #    fetched = class_type.fetch(session, default)
        #    if fetched:
        #        clean_main = fetched

        exist_param = {}
    """
    if not objects: return
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None
    class_type = None

    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            clean = obj.__clean__
            if not class_type:
                class_type = obj.__related__
        else:
            clean = obj
            obj = None
            if not class_type:
                class_type = clean.__class__

        if clean and clean not in clean_objects:
            clean_objects.append(clean)
            if len(clean.__raw__) > clean_cnt:
                clean_cnt = len(clean.__raw__)
                clean_main = clean
            # figures out the most frequent items
            if not keepexisting:
                for k in clean.__related__.summarize:
                    freq[k] += Counter(dict(clean.__rawgroup__(session, k)))
        elif obj and obj not in raw_objects:
            raw_objects.append(obj)

    exist_param = {}
    if clean_main:
        exist_param = clean_main.summarize

    if keepexisting:
        param = exist_param
    else:
        param = exist_param
        for obj in raw_objects:
            for k, v in obj.summarize.iteritems():
                if k not in default:
                    freq[k][v] += 1
            if "id" not in exist_param:
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
    param.update(default)

    # remove all clean objects
    if len(clean_objects) > 1:
        for obj in clean_objects:
            clean_main.relink(session, obj)
        session.commit()  # commit necessary

        # for some reason you need to delete this after the initial commit
        for obj in clean_objects:
            if obj != clean_main:
                session.delete(obj)

    if clean_main:
        relobj = clean_main
        relobj.update(**param)
    else:
        cleanObj = objects[0].__related__
        cleanCnt = session.query(cleanObj).filter(cleanObj.id == param["id"])
        if cleanCnt.count() > 0:
            relobj = cleanCnt.first()
            relobj.update(**param)
        else:
            relobj = cleanObj(**param)
    # associate the data into the related object

    for obj in raw_objects:
        relobj.relink(session, obj)

    session.merge(relobj)
    if commit:
        session.commit()


def unmatch(objects, session):
    """
    Separate our dataset

    # TODO
    # Unlinking doesn't seem to be working
    # properly if a LOCATION is added to items
    # such as Assignee/Lawyer
    """
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            obj.unlink(session)
        else:
            session.delete(obj)
            session.commit()

def commit_inserts(session, insert_statements, table, is_mysql, commit_frequency = 1000):
    """
    Executes bulk inserts for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. The insert_statement list of dictionaries may fall victim to SQLAlchemy
    complaining that certain columns are null, if you did not specify a value for every single
    column for a table.

    Args:
    session -- alchemy session object
    insert_statements -- list of dictionaries where each dictionary contains key-value pairs of the object
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table__
    is_mysql -- adjusts syntax based on if we are committing to MySQL or SQLite. You can use alchemy.is_mysql() to get this
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    if is_mysql:
        ignore_prefix = ("IGNORE",)
        session.execute("set foreign_key_checks = 0; set unique_checks = 0;")
        session.commit()
    else:
        ignore_prefix = ("OR IGNORE",)
    numgroups = len(insert_statements) / commit_frequency
    for ng in range(numgroups):
        if numgroups == 0:
            break
        chunk = insert_statements[ng*commit_frequency:(ng+1)*commit_frequency]
        session.connection().execute(table.insert(prefixes=ignore_prefix), chunk)
        print "committing chunk",ng+1,"of",numgroups,"with length",len(chunk),"at",datetime.now()
        session.commit()
    last_chunk = insert_statements[numgroups*commit_frequency:]
    if last_chunk:
        print "committing last",len(last_chunk),"records at",datetime.now()
        session.connection().execute(table.insert(prefixes=ignore_prefix), last_chunk)
        session.commit()

def commit_updates(session, update_key, update_statements, table, commit_frequency = 1000):
    """
    Executes bulk updates for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. In order to be flexible, the update statements must be set up in a specific
    way. You can only update one column at a time. The dictionaries in the list `update_statements`
    must have two keys: `pk`, which is the primary_key for the record to be updated, and `update`
    which is the new value for the column you want to change. The column you want to change
    is specified as a string by the argument `update_key`.

    This method will work regardless if you run it over MySQL or SQLite, but with MySQL, it is
    usually faster to use the bulk_commit_updates method (see lib/tasks.py), because it uses
    a table join to do the updates instead of executing individual statements.

    Args:
    session -- alchemy session object
    update_key -- the name of the column we want to update
    update_statements -- list of dictionaries of updates. See above description
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    primary_key = table.primary_key.columns.values()[0]
    update_key = table.columns[update_key]
    u = table.update().where(primary_key==bindparam('pk')).values({update_key: bindparam('update')})
    numgroups = len(update_statements) / commit_frequency
    for ng in range(numgroups):
        if numgroups == 0:
            break
        chunk = update_statements[ng*commit_frequency:(ng+1)*commit_frequency]
        session.connection().execute(u, *chunk)
        print "committing chunk",ng+1,"of",numgroups,"with length",len(chunk),"at",datetime.now()
        session.commit()
    last_chunk = update_statements[numgroups*commit_frequency:]
    if last_chunk:
        print "committing last",len(last_chunk),"records at",datetime.now()
        session.connection().execute(u, *last_chunk)
        session.commit()
