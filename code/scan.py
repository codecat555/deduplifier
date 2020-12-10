
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
EXCLUDED_TAGS = [ 59932, 59933, 'UserComment', 'MakerNote', 'ComponentsConfiguration' ]

mime = magic.Magic(mime=True)
hash_type = 'sha256'
hasher = FileHash(hash_type)

class InternalError(Exception):
    pass

def upsert_image_tags(conn, path, file_id, tags):
    print(f'upsert_image_tags({os.getpid()}): upserting with tags: {path}')

    agent_id = os.getpid()

    args = [
        file_id,
        [(key, str(value)) for key,value in tags.items()]
    ]
    print(f"upsert_image_tags() args:\n{args}")

    result = None
    with conn.cursor() as cursor:
        try:
            cursor.callproc('upsert_image_tags', args)
            result = cursor.fetchall()
            print(f"upsert_image_tags() result is {result}")
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(f'Error calling db: {error}')
            conn.rollback()
            raise

    assert(len(result) == len(tags))
    print(f"upsert_image_tags() result is {result}")
    return zip(tags, result)

def upsert_image(conn, path, file_id, tags):
    print(f'upsert_image({os.getpid()}): upserting with tags: {path}')

    agent_id = os.getpid()

    tag_ids = upsert_image_tags(conn, path, file_id, tags)

#    args = [
#        file_id,
#        (tags,)
#    ]
#    print(f"upsert_image() args:\n{args}")
#
#    result = None
#    with conn.cursor() as cursor:
#        try:
#            cursor.callproc('add_tags', args)
#            result = cursor.fetchall()
#            print(f"upsert_image() result is {result}")
#            conn.commit()
#        except(Exception, psycopg2.DatabaseError) as error:
#            print(f'Error calling db: {error}')
#            conn.rollback()
#            raise
#
#    assert(len(result) == len(tags))
#    print(f"upsert_image() result is {result}")
#    return zip(tags, result)
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
    #print(f'get_volname({os.getpid()}): path is {path}')
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

    #print(f'get_volname({os.getpid()}): result is {result}')
    return result

def upsert_file(conn, path, statinfo, hash, mimetype):
    print(f'upsert_file({os.getpid()}): upserting file with hash {hash} and type {mimetype} - {path}')

    agent_id = os.getpid()

    args = [
        str(agent_id),
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
    print(f"upsert_file() args:\n{args}")

    result = None
    with conn.cursor() as cursor:
        try:
            cursor.callproc('upsert_file', args)
            result = cursor.fetchone()
            print(f"upsert_file() result is {result}")
            conn.commit()
        except(Exception, psycopg2.DatabaseError) as error:
            print(f'Error calling db: {error}')
            conn.rollback()
            raise

    assert(len(result) == 1)
    return result[0]

def get_tags(path):
    tags = {}
    with Image.open(path) as image:
        exifdata = image.getexif()

        # now, handle the rest
        for tag_id in exifdata:
            tag = TAGS.get(tag_id, tag_id)

            if tag_id in EXCLUDED_TAGS or tag in EXCLUDED_TAGS:
                #print(f'get_tags({os.getpid()}):   - excluding tag: {tag}')
                continue
            assert tag != tag_id, f'unrecognized tag: {tag}'

            data = exifdata.get(tag_id)

            # handle gps data
            # see https://gist.github.com/erans/983821/e30bd051e1b1ae3cb07650f24184aa15c0037ce8
            if tag == 'GPSInfo':

                #print(f'get_tags({os.getpid()}):   - found GPSInfo tag')

                #print(f'get_tags({os.getpid()}):   - processing GPSInfo data')

                for gps_tag_id, gps_value in enumerate(data):
                    gps_tag = GPSTAGS.get(gps_tag_id, gps_tag_id)

                    #print(f'get_tags({os.getpid()}):   - found GPSInfo tag: {gps_tag}, with value {gps_value}.')

                    if isinstance(gps_value, bytes):
                        print(f'get_tags({os.getpid()}):   - here 0')
                        gps_value = gps_value.decode(errors='ignore')
                    elif isinstance(gps_value, str):
                        print(f'get_tags({os.getpid()}):   - here 1')
                        # trim strings from first null code
                        try:
                            idx = gps_value.index('\x00')
                            gps_value = gps_value[:idx]
                        except ValueError:
                            pass

                    assert(gps_tag not in tags)
                    tags[gps_tag] = gps_value

                    print(f'get_tags({os.getpid()}): {gps_tag}: {gps_value}')
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
                    print(f'get_tags({os.getpid()}): {tag}: {data} ({type(data)})')

                # WIP - remove this if later, once all other tags seem to be working
                if tag != 'UserComment' and tag != 'MakerNote':
                    assert(tag not in tags)
                    tags[tag] = data

        print()
        return tags

def process_image(conn, path, file_id, mimetype):
    print(f'process_image({os.getpid()}): processing image file: {path} ({mimetype})')

    # capture (and filter) the image tags, for upload
    tags = get_tags(path)

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

    image_id = None
    if (mimetype.split('/'))[0] == 'image':
        image_id = process_image(conn, path, file_id, mimetype)
    else:
        print(f'process_file({os.getpid()}): skipping file: {path} ({mimetype})')

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
        raise
    except (psycopg2.Error) as error:
        print(f'Error: {error}')
        if conn:
            conn.rollback()
        raise
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

