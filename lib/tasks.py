"""
This module uses the Celery task manager to dispatch parallel database tasks to help speed up the task
of performing multiple updates over multiple tables.
"""
import celery
from alchemy.match import commit_inserts, commit_updates
from alchemy import session_generator
from alchemy.schema import temporary_update, app_temporary_update
from sqlalchemy import create_engine, MetaData, Table, inspect, VARCHAR, Column
from sqlalchemy.orm import sessionmaker

# fetch reference to temporary_update table.
celery = celery.Celery('tasks', broker='redis://localhost', backend='redis://localhost')

@celery.task
def celery_commit_inserts(insert_statements, table, is_mysql, commit_frequency = 1000, dbtype='grant'):
    """
    Executes bulk inserts for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. The insert_statement list of dictionaries may fall victim to SQLAlchemy
    complaining that certain columns are null, if you did not specify a value for every single
    column for a table.

    A session is generated using the scoped_session factory through SQLAlchemy, and then
    the actual lib.alchemy.match.commit_inserts task is dispatched.

    Args:
    insert_statements -- list of dictionaries where each dictionary contains key-value pairs of the object
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table__
    is_mysql -- adjusts syntax based on if we are committing to MySQL or SQLite. You can use alchemy.is_mysql() to get this
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    session = session_generator(dbtype=dbtype)
    commit_inserts(session, insert_statements, table, is_mysql, commit_frequency)

@celery.task
def celery_commit_updates(update_key, update_statements, table, is_mysql, commit_frequency = 1000, dbtype='grant'):
    """
    Executes bulk updates for a given table. This is typically much faster than going through
    the SQLAlchemy ORM. In order to be flexible, the update statements must be set up in a specific
    way. You can only update one column at a time. The dictionaries in the list `update_statements`
    must have two keys: `pk`, which is the primary_key for the record to be updated, and `update`
    which is the new value for the column you want to change. The column you want to change
    is specified as a string by the argument `update_key`.

    If is_mysql is True, then the update will be performed by inserting the record updates
    into the table temporary_update and then executing an UPDATE/JOIN. If is_mysql is False,
    then SQLite is assumed, and traditional updates are used (lib.alchemy.match.commit_updates)

    A session is generated using the scoped_session factory through SQLAlchemy, and then
    the actual task is dispatched.

    Args:
    update_key -- the name of the column we want to update
    update_statements -- list of dictionaries of updates. See above description
    table -- SQLAlchemy table object. If you have a table reference, you can use TableName.__table
    commit_frequency -- tune this for speed. Runs "session.commit" every `commit_frequency` items
    """
    session = session_generator(dbtype=dbtype)
    if not is_mysql:
        commit_updates(session, update_key, update_statements, table, commit_frequency)
        return
    session.rollback()
    session.execute('truncate temporary_update;')
    if dbtype == 'grant':
        commit_inserts(session, update_statements, temporary_update, is_mysql, 10000)
    else:
        commit_inserts(session, update_statements, app_temporary_update, is_mysql, 10000)
    # now update using the join
    primary_key = table.primary_key.columns.values()[0]
    update_key = table.columns[update_key]
    session.execute("UPDATE {0} join temporary_update ON temporary_update.pk = {1} SET {2} = temporary_update.update;".format(table.name, primary_key.name, update_key.name ))
    session.commit()
    session.execute("truncate temporary_update;")
    session.commit()
