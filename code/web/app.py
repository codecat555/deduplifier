
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
                result = cursor.fetchall()[0]
                cursor.close()
        finally:
            # connection.close()
            pass

        return result

    def list_files_with_dups(self, start_idx, rows_per_page, sort_field, sort_direction):
        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.callproc('files_with_dups', [ start_idx, rows_per_page, sort_field, sort_direction ])
                headers = [(desc.name, desc.name.replace('_', ' ').title()) for desc in cursor.description]
                result = (headers, cursor.fetchall())
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


@app.route('/files_with_dups', defaults={ 'start_row': 1, 'rows_per_page': 20, 'sort_field': 'bytes', 'sort_direction': 'desc' }, methods=['GET', 'POST'])
def files_with_dups(start_row, rows_per_page, sort_field, sort_direction):


    result = None
    testing = False
    if testing:
        headers = ['#', 'Duplicates', 'Total Mb', 'File Name']
        result = [[7, 10661405447, 'Teeth Main Assem - Lower Gum-2 Mirror3[13]-1.STL'], [7, 10660059109, 'Teeth Main Assem - Lower Gum-2 Imported14-1.STL'], [7, 10571394225, 'Teeth Main Assem - Lower Gum-2 Lower Gum-1.STL'], [7, 10256363012, 'Teeth Main Assem - Lower Gum-2 Imported18-1.STL'], [7, 10255755419, 'Teeth Main Assem - Lower Gum-2 Mirror3[1]-1.STL'], [7, 10200022665, 'Teeth Main Assem - Lower Gum-2 Imported2-1.STL'], [7, 10014125380, 'Planmeca_Imaging_Package_2018-01.zip'], [53, 9997525203, 'Teeth Main Assem.STEP'], [5, 8855311995, 'genTB.log.old'], [6, 8367710208, '6.0.6001.18000.367-KRMSDK_EN.iso'], [12, 6996737532, 'MFC group picture - 1989.psd'], [7, 6598810624, 'alt-outlook.pst'], [23, 6427305472, 'NONAME_001.AVI'], [12, 6411512220, 'MFC group picture - 1990.psd'], [7, 5522404020, 'Teeth Main Assem - Lower Gum-2 Mirror3[10]-1.STL'], [7, 5520882780, 'Teeth Main Assem - Lower Gum-2 Imported10-1.STL'], [8, 5481625992, 'Mandible_FMA52748_2mm.stp'], [3, 4815689013, '014.mp4'], [5, 4713436160, 'outlook.pst'], [15, 4564638720, 'outlook.pst']]
    else:
        db = app_db(connection_parameters)
        if request.method == 'POST':
            sort_field = request.form['sort_field']
            sort_direction = request.form['sort_direction']

            start_row = int(request.form['start_row'])
            rows_per_page = int(request.form['rows_per_page'])
            if 'pager' in request.form:
                if request.form['pager'] == 'previous':
                    start_row = max(start_row-rows_per_page, 0)
                elif request.form['pager'] == 'next':
                    start_row += rows_per_page
                else:
                    raise BadRequest(str(request))
        
        headers, result = db.list_files_with_dups(start_row-1, rows_per_page, sort_field, sort_direction)
        # print('headers is ' + str(headers), file=sys.stderr)

    previous_page_status = ''
    if start_row == 1:
        # disable previous button
        previous_page_status = 'disabled' 

    next_page_status = ''
    if len(result) < rows_per_page:
        # disable next button
        next_page_status = 'disabled' ,
        
    # adjust "bytes" to mb
    print('result is ' + str(result), file=sys.stderr)    
    result = [[r[0], f'{ (r[1] / 1048576):.0f}', r[2]] for r in result]
    
    return render_template(
            'list_files_with_dups.html',
            title='Duplicate Files',
            description='Listing of files having one or more duplicates',
            start_row=start_row,
            sort_field=sort_field,
            sort_direction=sort_direction,
            rows_per_page=rows_per_page,
            row_count=len(result),
            post_url='/files_with_dups',
            next_page_status=next_page_status, 
            previous_page_status=previous_page_status, 
            headers=headers,
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

