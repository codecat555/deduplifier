
import sys
import time
import os

#sys.path.append('/app/code')

import redis
cache = redis.Redis(host='redis', port=6379)

from flask import Flask, render_template, request
from werkzeug.exceptions import BadRequest
app = Flask(__name__, static_url_path='/static')

# from https://flask.palletsprojects.com/en/2.0.x/errorhandling
@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return 'bad bad request!', 400
    
import psycopg2
from psycopg2 import Error
import psycopg2.extras

#from db_config import connection_parameters
connection_parameters = dict(
    user = "postgres",
    password = "postgres",
    host = "db",
    # use db internal port here
    port = "3368",
    # database used for testing
    # database = "boffo"
    database = "deduplifier"
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

        result = self.get_totals()
        if int(result[1]) == 0:
            template_name = 'db_summary-empty.html'
        else:
            template_name = 'db_summary.html'

        return render_template(
            template_name,
            title='Deduplifier Summary',
            description='Summary of Deduplifier database contents',
            dbname=self.conn.get_dsn_parameters()['dbname'],
            total_count=result[1],
            total_size=result[2]
        )

    def __del__(self):
        if self.conn:
            self.conn.close()

    def get_totals(self):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('get_totals', [])
                # result = cursor.fetchone()
                result = cursor.fetchall()[0]
                cursor.close()
        finally:
            # connection.close()
            pass

        return result

    def get_counts(self):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('get_counts', [])
                # result = cursor.fetchall()
                result = cursor.fetchall()[0]
                cursor.close()
        finally:
            # connection.close()
            pass

        return result

    def list_files_with_dups(self, start_idx=1, rows_per_page=20):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.callproc('files_with_dups', [ start_idx, rows_per_page ])
                result = cursor.fetchall()
        finally:
            # connection.close()
            pass

        return result

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

@app.route('/files_with_dups', defaults={ 'start_row': 1, 'rows_per_page': 20 }, methods=['GET', 'POST'])
# @app.route('/files_with_dups/<int:start_idx>/<int:rows_per_page>')
def files_with_dups(start_row, rows_per_page):
    db = app_db(connection_parameters)
    if request.method == 'POST':
        start_row = int(request.form['start_row'])
        rows_per_page = int(request.form['rows_per_page'])
        if 'pager' in request.form:
            if request.form['pager'] == 'previous':
                start_row = max(start_row-rows_per_page, 0)
            elif request.form['pager'] == 'next':
                start_row += rows_per_page
            else:
                raise BadRequest(str(request))
        
    result = db.list_files_with_dups(start_row-1, rows_per_page)

    previous_page_status = ''
    if start_row == 1:
        # disable previous button
        previous_page_status = 'disabled' 

    next_page_status = ''
    if len(result) < rows_per_page:
        # disable next button
        next_page_status = 'disabled' ,
        
    # adjust "bytes" to mb
    # print('result is ' + str(result), file=sys.stderr)    
    result = [[r[0], f'{ (r[1] / 1048576):.0f}', r[2]] for r in result]
    
    return render_template(
            'list_files_with_dups.html',
            title='Duplicate Files',
            description='List of Known Files with counts of duplicates',
            start_row=start_row,
            rows_per_page=rows_per_page,
            row_count=len(result),
            next_page_status=next_page_status, 
            previous_page_status=previous_page_status, 
            files=result
    )

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

# @app.route('/doc/scan')
# def scan_doc():
#     # note: the path here is relative to the 'static' sub-directory located next to app.py
#     return app.send_static_file('scan.html')

#@app.route('/dump')
#def pwd():
#    return dump()

if __name__ == '__main__':
    app.run(debug=True)

