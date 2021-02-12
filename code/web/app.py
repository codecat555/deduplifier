
import sys
import time
import os

#sys.path.append('/app/code')

import redis
cache = redis.Redis(host='redis', port=6379)

from flask import Flask, render_template
app = Flask(__name__)

import psycopg2
from psycopg2 import Error
import psycopg2.extras

#from db_config import connection_parameters
connection_parameters = dict(
    user = "postgres",
    password = "postgres",
    host = "db",
    #port = "6681",
    port = "3368",
    #database = "deduplifier"
    database = "boffo"
)

class app_db:
    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters
        self.pid = os.getpid()

        # we delay opening a connection until the first time we actually need it
        self.conn = None

    def welcome(self):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        result = self.get_counts()
        if result[0] == 0:
            template_name = 'db_summary-empty.html'
        else:
            template_name = 'db_summary.html'

        return render_template(
            template_name,
            title='Deduplifier Summary',
            description='Summary of Deduplifier database contents',
            dbname=connection_parameters['database'],
            file_count=result
        )

    def __del__(self):
        if self.conn:
            self.conn.close()

    def get_counts(self):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('get_counts', [])
                result = cursor.fetchone()[0]
        finally:
            self.conn.rollback()

        return result

    def list_files_with_dups(self, start_idx=1, rows_per_page=20):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.callproc('files_with_dups', [ start_idx, rows_per_page ])
                result = cursor.fetchall()
        finally:
            self.conn.rollback()

        return render_template(
            'list_files_with_dups.html',
            title='Duplicate Files',
            description='List of Known Files with counts of duplicates',
            start_idx=start_idx,
            rows_per_page=rows_per_page,
            row_count=len(result),
            files=result
        )

def get_hit_count():
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)

@app.route('/')
def welcome():
    # show summary screen
    # - counts of files
    # - by mime type
    # - by volume
    # - query button
    # -- form a query to select data
    # enable forming two sets from the duplicates: primary and secondary copies.
    # - everything else gets deleted.
    # - identify which volumes contain the most duplicates and elminate them first?
    db = app_db(connection_parameters)
    #count = get_hit_count() + 100
    #return 'Hello World! I have been seen {} times.\n'.format(count)
    return db.welcome()

@app.route('/files_with_dups')
def files_with_dups():
    db = app_db(connection_parameters)
    return db.list_files_with_dups()

@app.route('/hello')
def hello():
    count = get_hit_count()
    return 'Hello World! I have been seen {} times.\n'.format(count)

@app.route('/pwd')
def pwd():
    return f'Current directory is {os.getcwd()}.'

@app.route('/testit')
def testit():
    return ',   '.join(os.listdir('web'))

#@app.route('/dump')
#def pwd():
#    return dump()

if __name__ == '__main__':
    app.run(debug=True)

