from celery import Celery
from lib.alchemy.match import commit_inserts, commit_updates
from lib.alchemy import session_generator

app = Celery('tasks', broker='redis://localhost')
sess_gen = session_generator()

@app.task
def celery_commit_inserts(insert_statements, table, is_mysql, commit_frequency = 1000):
    commit_inserts(sess_gen(), insert_statements, table, is_mysql, commit_frequency)
    
@app.task
def celery_commit_updates(update_key, update_statements, table, commit_frequency = 1000):
    commit_updates(sess_gen(), update_key, update_statements, table, commit_frequency)
