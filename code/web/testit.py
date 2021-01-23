
import sys
import time
import os

sys.path.append('/app/code')

import redis
cache = redis.Redis(host='redis', port=6379)

from flask import Flask, render_template
app = Flask(__name__)

import psycopg2
from psycopg2 import Error
from db_config import connection_parameters

class app_db:
    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters
        self.pid = os.getpid()

        # we delay opening a connection until the first time we actually need it
        self.conn = None

    def testit(self, startat=1, rows_per_page=20):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        if not self.list_files_cursor:
            self.list_files_cursor = self.conn.cursor(name='files_with_dups', scrollable=True)

        try:
            with self.conn.cursor(name='files_with_dups', scrollable=True) as cursor:
                cursor.arraysize = rows_per_page
                cursor.callproc('files_with_dups', [ startat, ])
                result = cursor.fetchmany()
        finally:
            self.conn.rollback()

        return result

    def __del__(self):
        if self.list_files_cursor:
            self.conn.close()
        if self.conn:
            self.conn.close()

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

