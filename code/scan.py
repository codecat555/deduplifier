
#
# install required modules: 
#   python3 -m pip install --upgrade pip
#   pip install python-magic-bin
#   pip install Pillow
#   pip install psycopg2
#   pip install filehash
#
import sys
import os
import stat
import time

import platform

import subprocess

import multiprocessing as mp

import magic

# from package Pillow
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

import psycopg2
from psycopg2 import Error
from db_config import connection_parameters

#import imagehash

from filehash import FileHash

#if platform.system() == 'Windows':
#    import wmi
#    wmiobj = wmi.WMI()

WORKER_COUNT = 1
MAIN_LOOP_SLEEP_INTERVAL = 3
MAX_IDLE_ITERATIONS=4
QUEUE_DRAIN_SLEEP_INTERVAL = 1
EXCLUDED_TAGS = [ 59932, 59933 ]

mime = magic.Magic(mime=True)
hash_type = 'sha256'
hasher = FileHash(hash_type)

class InternalError(Exception):
    pass
#    def __str__(self):
#        return "Internal error: 

################

#def add_file(*add_file_args):
#    cursor = conn.cursor()
#    try:
#        cursor.callproc('add_file', *add_file_args)
#        result = cursor.fetchall()
#        print(f"add_file() result is {result}")
#
#        conn.commit()
#    except(Exception, psycopg2.DatabaseError) as error:
#        print(f'Error calling db: {error}')
#        conn.rollback()
#    finally:
#        if(conn):
#            cursor.close()
#            conn.close()
#
#if __name__ == '__main__':
#    try:
#        conn = psycopg2.connect(**connection_parameters)
#    except (Exception, psycopg2.Error) as e:
#        print(f'Error connecting to db: {e}')
#
#    args = [
#        'myagent',
#        'myhost',
#        'drivename',
#        'volname',
#        '/a/d/a/x',
#        '/',
#        'lola.jpg',
#        'ff15659bad5d6090dccfaa0f4208a7d0a201fcda',
#        'SHA1',
#        '11/25/2020 2:15',
#        '11/25/2020 2:15',
#        '11/25/2020 2:15',
#        '11/25/2020 2:15'
#    ]
#    if len(sys.argv) > 1:
#        args = sys.argv[1:]
#
#    add_file(args)

################

def upsert_image(conn, path, file_id, tags):
    print(f'upsert_image({os.getpid()}): upserting with tags: {path}')

#    with conn.cursor() as cursor:
#        cursor.execute("CALL add_image(%s)", tags)
#
#        #cursor.execute("CALL upsert_tags(%s)", tags)

    return 1

def get_drivename(path):
    print(f'get_drivename({os.getpid()}): path is {path}')
    if platform.system() == 'Linux':
        result = 1
    elif platform.system() == 'Windows':
        #assert(wmiobj is not None)
        #raise Exception("Windows support not implemented yet, can't get drive name")
        result = 1
    else:
        raise Exception(f"unsupported system ({platform.system()}), can't get drive name")

    print(f'get_drivename({os.getpid()}): result is {result}')
    return result

def get_volname(path):
    print(f'get_volname({os.getpid()}): path is {path}')
    if platform.system() == 'Linux':
        # see https://askubuntu.com/questions/1096813/how-to-get-uuid-partition-form-a-given-file-path-in-python
        completed_process = subprocess.run(
            ['/usr/bin/bash', '-c', f'/usr/sbin/blkid -o value $(/usr/bin/df --output=source {path} | tail -1) | head -1'],
            check=True,
            text=True,
            capture_output=True
        )
        result = completed_process.stdout.rstrip()
    elif platform.system() == 'Windows':
## successful example
##  >>> subprocess.run(["powershell", "-Command", r"(Get-Partition -DriveLetter ([System.IO.Path]::GetPathRoot('C:\Users\me\Pictures\2015-02-21\20150221_135026.jpg').Split(':')[0])).Guid" ], capture_output=True, text=True)
##  CompletedProcess(args=['powershell', '-Command', "(Get-Partition -DriveLetter ([System.IO.Path]::GetPathRoot('C:\\Users\\me\\Pictures\\2015-02-21\\20150221_135026.jpg').Split(':')[0])).Guid"], returncode=0, stdout='{445e65f9-aabb-4531-b4b8-7745f786cd96}\n', stderr='')
#
#        completed_process = subprocess.run(["powershell", "-Command", r"(Get-Partition -DriveLetter ([System.IO.Path]::GetPathRoot('C:\Users\me\Pictures\2015-02-21\20150221_135026.jpg').Split(':')[0])).Guid" ], capture_output=True, text=True, check=True)
#        
#        completed_process = subprocess.run(
#        [r'powershell.exe', r'-Command', r"(Get-Partition -DriveLetter ([System.IO.Path]::GetPathRoot('C:\Users\me\Pictures\2015-02-21\20150221_135026.jpg').Split(':')[0])).Guid"],
#            check=True,
#            text=True,
#            capture_output=True
#        )
#
#
#  >>> path = r'C:\Users\me\Pictures\2015-02-21\20150221_135026.jpg'
#  >>> command_string = f"$driveLetter = [System.IO.Path]::GetPathRoot('{path}').Split(':')[0]; (Get-Partition -DriveLetter $driveLetter).Guid"
#  >>> completed_process = subprocess.run(
#  ...     [r'powershell.exe', r'-Command', command_string],
#  ...     #check=True,
#  ...     text=True,
#  ...     capture_output=True
#  ... )
#  >>> print(completed_process.stdout)
#  {445e65f9-aabb-4531-b4b8-7745f786cd96}
#
#
#  >>> path = r'\\nuage\me\Documents\dev\deduplifier\code\testdata\2020-11-21\009.jpg'
#  >>> command_string = f"$driveLetter = [System.IO.Path]::GetPathRoot('{path}').Split(':')[0]; (Get-Partition -DriveLetter $driveLetter).Guid"
#  >>> completed_process = None
#  >>> completed_process = subprocess.run(
#  ...     [r'powershell.exe', r'-Command', command_string],
#  ...     #check=True,
#  ...     text=True,
#  ...     capture_output=True
#  ... )
#  >>> print(completed_process.stdout)
#  {f51db7df-56af-4da4-801c-a9afdd3a8802}
#  >>> print(completed_process.stdout.strip('{}\n'))
#  f51db7df-56af-4da4-801c-a9afdd3a8802
#  >>>
#  >>>
#
#
        command_string = f"$driveLetter = [System.IO.Path]::GetPathRoot('{path}').Split(':')[0]; (Get-Partition -DriveLetter $driveLetter).Guid"
        completed_process = subprocess.run(
            [r'powershell.exe', r'-Command', command_string],
            check=True,
            text=True,
            capture_output=True
        )
        #print(f'get_volname({os.getpid()}): raw output is {completed_process.stdout}')
        result = completed_process.stdout.strip('{}\n')
    else:
        raise Exception(f"unsupported system ({platform.system()}), can't get volume name")

    print(f'get_volname({os.getpid()}): result is {result}')
    return result

def upsert_file(conn, path, statinfo, hash, mimetype):
    print(f'upsert_file({os.getpid()}): upserting file with hash {hash} and type {mimetype} - {path}')

    agent_id = os.getpid()

    args = [
        agent_id,
        platform.node(),
        #get_drivename(path),
        get_volname(path),
        os.path.dirname(path),
        os.path.sep,
        os.path.basename(path),
        hash,
        hash_type,
        time.ctime(statinfo.st_ctime),
        time.ctime(statinfo.st_mtime),
        time.ctime(statinfo.st_atime),
        time.ctime()
    ]

    with conn.cursor() as cursor:
        try:
            cursor.callproc('add_file', *args)
            result = cursor.fetchall()
            print(f"add_file() result is {result}")
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(f'Error calling db: {error}')
            conn.rollback()

    return 1

def upsert_image_file(conn, file_id, image_id):
    print(f'upsert_image_file({os.getpid()}): upserting image-file relation between image {image_id} and file {file_id}')
    pass

def process_image_file(conn, path, file_id, mimetype):
    print(f'process_image_file({os.getpid()}): processing image file: {path} ({mimetype})')

    # capture (and filter) the image tags, for upload
    tags = {}
    with Image.open(path) as image:
        exifdata = image.getexif()

        # now, handle the rest
        for tag_id in exifdata:
            tag = TAGS.get(tag_id, tag_id)

            if tag_id in EXCLUDED_TAGS or tag in EXCLUDED_TAGS:
                #print(f'process_image_file({os.getpid()}):   - excluding tag: {tag}')
                continue
            assert tag != tag_id, f'unrecognized tag: {tag}'

            data = exifdata.get(tag_id)

            # handle gps data
            # see https://gist.github.com/erans/983821/e30bd051e1b1ae3cb07650f24184aa15c0037ce8
            if tag == 'GPSInfo':

                #print(f'process_image_file({os.getpid()}):   - found GPSInfo tag')

                #print(f'process_image_file({os.getpid()}):   - processing GPSInfo data')

                for gps_tag_id, gps_value in enumerate(data):
                    gps_tag = GPSTAGS.get(gps_tag_id, gps_tag_id)

                    #print(f'process_image_file({os.getpid()}):   - found GPSInfo tag: {gps_tag}, with value {gps_value}.')

                    if isinstance(gps_value, bytes):
                        print(f'process_image_file({os.getpid()}):   - here 0')
                        gps_value = gps_value.decode(errors='ignore')
                    elif isinstance(gps_value, str):
                        print(f'process_image_file({os.getpid()}):   - here 1')
                        # trim strings from first null code
                        try:
                            idx = gps_value.index('\x00')
                            gps_value = gps_value[:idx]
                        except ValueError:
                            pass

                    assert(gps_tag not in tags)
                    tags[gps_tag] = gps_value

                    print(f'process_image_file({os.getpid()}): {gps_tag}: {gps_value}')
            else:
                if isinstance(data, bytes):
                    data = data.decode(errors='ignore')
                elif isinstance(data, str):
                    # trim strings from first null code
                    try:
                        idx = data.index('\x00')
                        data = data[:idx]
                    except ValueError:
                        pass

                #if tag != 'UserComment' and tag != 'ComponentsConfiguration':
                if tag != 'UserComment' and tag != 'MakerNote':
                    print(f'process_image_file({os.getpid()}): {tag}: {data}')

                assert(tag not in tags)
                tags[tag] = data
        print()

    # upsert image in db
    image_id = upsert_image(conn, path, file_id, tags)

    # return db image id
    return image_id

def process_file(conn, path, statinfo):
    hash = hasher.hash_file(path)

    mimetype = mime.from_file(path)

    # upsert file
    file_id = upsert_file(conn, path, statinfo, hash, mimetype)
    print(f'process_file({os.getpid()}): upserted file: {file_id}')
    return

    image_id = None
    if (mimetype.split('/'))[0] == 'image':
        image_id = process_image_file(conn, path, file_id, mimetype)
    else:
        print(f'process_file({os.getpid()}): skipping file: {path} ({mimetype})')

    # upsert image-file relation
    if not file_id is None and not image_id is None: 
        upsert_image_file(conn, file_id, image_id)

def scan(workq, idle_worker_count):
    pid = os.getpid()

    conn = None
    error = None
    try:
        conn = psycopg2.connect(**connection_parameters)

        while True:
            print(f'scan({pid}): fetching new item...')

            item = workq.get()

            if item is None:
                print(f'scan({pid}): found end-of-queue, quitting...')
                break

            with idle_worker_count.get_lock():
                idle_worker_count.value -= 1

            print(f'scan({pid}): ITEM IS {item}')
            printed_banner = False
            try:
                # assume item is a directory and try scanning it
                for entry in os.scandir(item):
                    if not printed_banner:
                        print(f'scan({pid}): scan: processing directory: {item}')
                        printed_banner = True

                    if entry.is_file(follow_symlinks=False):
                        workq.put(entry.path)
                    elif entry.is_dir(follow_symlinks=False):
                        workq.put(entry.path)
                    else:
                        print(f'scan({pid}): scan: {entry.path} - not a file or dir, skipping...')
            except NotADirectoryError:
                # is current item a file?
                statinfo = os.stat(item)
                if stat.S_ISREG(statinfo.st_mode):
                    process_file(conn, item, statinfo)
                else:
                    workq.task_done()
                    with idle_worker_count.get_lock():
                        idle_worker_count.value += 1

                    raise InternalError('Internal error: unknown item type found in work queue')

            workq.task_done()
            with idle_worker_count.get_lock():
                idle_worker_count.value += 1

            print()

        print()
        print(f'scan({pid}): scan: at end of loop, workq is {workq.qsize()}, empty = {workq.empty()}')
        print()
    except(psycopg2.DatabaseError) as error:
        print(f'Database error: {error}')
        if conn:
            conn.rollback()
    except (psycopg2.Error) as error:
        print(f'Error: {error}')
        if conn:
            conn.rollback()
    except Exception as error:
        print(f'scan({pid}): here, error is {error}')
        if (error):
            print(f'scan({pid}): getting idle lock')
            with idle_worker_count.get_lock():
                idle_worker_count.value += 1
            print(f'scan({pid}): idled and exiting...')
            exit(1)
    finally:
        workq.close()
        if(conn):
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} <target-directory>[,<target-directory>...]', file=sys.stderr)
        exit(1)

    # create the work queue
    workq = mp.JoinableQueue()
    idle_worker_count = mp.Value('i', WORKER_COUNT)

    # prime the work queue with the list of target directories
    for path in sys.argv[1:]:
        workq.put(os.path.abspath(path))

    pid = os.getpid()
    print(f'main({pid}): starting...')

    with mp.Pool(WORKER_COUNT, scan, (workq, idle_worker_count)) as pool:
        time.sleep(2)

        idle_iterations = 0
        while True:
            print(f'main({pid}): idle_worker_count is {idle_worker_count.value}, workq size is {workq.qsize()}')
            if (idle_worker_count.value == WORKER_COUNT):
                idle_iterations += 1

                print(f'main({pid}): all idle (qsize is {workq.qsize()}, idle iteration count is {idle_iterations})...')
                if workq.qsize() == 0:
                    # all idle and nothing left to do -> shut it down
                    print(f'main({pid}): shutting down 0 (qsize is {workq.qsize()})...')
                    for i in range(WORKER_COUNT):
                        workq.put(None)

                    while workq.qsize() > 0:
                        time.sleep(QUEUE_DRAIN_SLEEP_INTERVAL)

                    print(f'main({pid}): shutting down 1 (qsize is {workq.qsize()})...')
                    workq.close()

                    print(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                    break
                elif idle_iterations >= MAX_IDLE_ITERATIONS:
                    # all idle and no progress -> shut it down
                    print(f'main({pid}): shutting down 0 (qsize is {workq.qsize()})...')

                    for i in range(WORKER_COUNT):
                        workq.put(None)

                    time.sleep(QUEUE_DRAIN_SLEEP_INTERVAL)

                    print(f'main({pid}): shutting down 1 (qsize is {workq.qsize()})...')
                    workq.close()

                    print(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                    break
            else:
                idle_iterations = 0

                print(f'main({pid}): sleeping...')
                time.sleep(MAIN_LOOP_SLEEP_INTERVAL)

        print(f'main({pid}): shutting down 3...')

        if idle_iterations >= MAX_IDLE_ITERATIONS:
            raise Exception('all idle and no progress, giving up')

    #scan(workq)

    print(f'main({pid}): all done.')

