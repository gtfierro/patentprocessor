import celery
from alchemy.match import commit_inserts, commit_updates
from alchemy import session_generator
from sqlalchemy import create_engine, MetaData, Table, inspect, VARCHAR, Column
from sqlalchemy.orm import sessionmaker

app = celery.Celery('tasks', broker='redis://localhost', backend='redis://localhost')
#sess_gen = session_generator()
engine = create_engine('mysql+mysqldb://root:330Ablumhall@169.229.7.251/usptofixed?charset=utf8')
metadata = MetaData(engine)
temporary_update = Table('temporary_update', metadata, autoload=True)
#temporary_update.create()

@app.task
def celery_commit_inserts(insert_statements, table, is_mysql, commit_frequency = 1000):
    session = session_generator()
    commit_inserts(session, insert_statements, table, is_mysql, commit_frequency)

@app.task
def celery_commit_updates(update_key, update_statements, table, is_mysql, commit_frequency = 1000):
    #session = sess_gen()
    session = session_generator()
    commit_inserts(session, update_statements, temporary_update, is_mysql, 10000)
    # now update using the join
    #session = session_generator()
    primary_key = table.primary_key.columns.values()[0]
    update_key = table.columns[update_key]
    session.execute("UPDATE {0} join temporary_update ON temporary_update.pk = {1} SET {2} = temporary_update.update;".format(table.name, primary_key.name, update_key.name ))
    session.commit()
    session.execute("drop table temporary_update;")
    session.commit()
