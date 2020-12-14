
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

import logging
import logging.handlers
import multiprocessing

import re

import magic

# from package Pillow
from PIL import Image
Image.MAX_IMAGE_PIXELS = 2000000000
from PIL.TiffImagePlugin import IFDRational
from PIL.ExifTags import TAGS, GPSTAGS

import psycopg2
from psycopg2 import Error
from db_config import connection_parameters

#import imagehash

from filehash import FileHash

import shlex

#if platform.system() == 'Windows':
#    import wmi
#    wmiobj = wmi.WMI()

MAIN_LOOP_SLEEP_INTERVAL = 3
MAX_IDLE_ITERATIONS=4
QUEUE_DRAIN_SLEEP_INTERVAL = 1

LOG_FORMAT='%(asctime)s %(levelname)s %(message)s'

# we skip files bigger than this size...useful during development but
# maybe not in production.
FILE_SIZE_THRESHOLD = 2 * 1024*1024*1024

# tag types for image metadata. would be nice if pillow handled the conversions rather than
# just returning bytes values...other module seem to do so. maybe I missed something in the
# doc?...no, don't think so.
EXCLUDED_TAGS = [ 59932, 59933, 'UserComment', 'MakerNote', ]
#INT_TAGS = [ 'SceneType', 'GPSAltitudeRef', 'GPSDifferential' ]
INT_TAGS = [ 'GPSAltitudeRef', 'GPSDifferential' ]
TUPLE_TAGS = [ 'ComponentsConfiguration', 'GPSVersionID' ]

EXCLUDE_PATTERNS = [ '^NTUSER.DAT' ]
EXCLUDE_REGEX = []
for p in EXCLUDE_PATTERNS:
    EXCLUDE_REGEX.append(re.compile(p))

worker_count = 0

mime = magic.Magic(mime=True)
hash_type = 'sha256'
hasher = FileHash(hash_type)

logger = None

class InternalError(Exception):
    pass

class ExcludedFile(Exception):
    pass

# from https://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python
def exception_handler(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

def excluded(string):
    for pat in EXCLUDE_REGEX:
        if pat.match(string):
            return True
    return False

def upsert_image_tags(conn, path, img_id, tags):
    logger = logging.getLogger(__name__)

    logger.info(f'upsert_image_tags({os.getpid()}): upserting image tags: {path}')

    agent_id = os.getpid()

    args = [
        img_id,
        [(key, value) for key,value in tags.items()]
    ]
    #if len(args[1]) == 0:
    #    args[1] = 'NULL'
    logger.info(f"upsert_image_tags() args: [{', '.join([str(arg) for arg in args])}]")

    result = None
    with conn.cursor() as cursor:
        try:
            #cursor.callproc('upsert_image_tags', args)
            sql = bytearray(cursor.mogrify('SELECT upsert_image_tags(%s, %s)', args))
            logger.debug(f"upsert_image_tags({os.getpid()}): sql is '{sql}'")
            assert(sql[-1:] == b')')
            sql[-1:] = b'::image_tag_type[])'
            sql = bytes(sql)
            cursor.execute(sql)
            result = cursor.fetchall()[0][0]
            #logger.debug(f"upsert_image_tags() result is {result}")
            conn.commit()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            conn.rollback()
            raise

    assert(len(tags) == len(result))
    return zip(tags, result)

def upsert_image(conn, path, file_id, tags):
    logger = logging.getLogger(__name__)
    logger.info(f'upsert_image({os.getpid()}): upserting image: {path}')

    agent_id = os.getpid()

    # fake imagehash for now
    #imghash = str(time.time())
    imghash = None

    args = [
        file_id,
        imghash
    ]
    logger.info(f"upsert_image() args: [{', '.join([str(arg) for arg in args])}]")

    img_id = None
    with conn.cursor() as cursor:
        try:
            cursor.callproc('upsert_image', args)
            img_id = cursor.fetchone()[0]
            logger.info(f"upsert_image() img_id is {img_id}")
            conn.commit()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            conn.rollback()
            raise

    if len(tags) > 0:
        tag_ids = upsert_image_tags(conn, path, img_id, tags)
        logger.info(f"upsert_image() tag_ids are {tag_ids}")

    return 1

def get_drivename(path):
    logger = logging.getLogger(__name__)
    logger.info(f'get_drivename({os.getpid()}): path is {path}')
    if platform.system() == 'Linux':
        result = 1
    elif platform.system() == 'Windows':
        #assert(wmiobj is not None)
        #raise Exception("Windows support not implemented yet, can't get drive name")
        result = 1
    else:
        raise Exception(f"unsupported system ({platform.system()}), can't get drive name")

    #logger.debug(f'get_drivename({os.getpid()}): result is {result}')
    return result

def get_volname(path):
    logger = logging.getLogger(__name__)
    #logger.info(f'get_volname({os.getpid()}): path is {path}')
    if platform.system() == 'Linux':
        # see https://askubuntu.com/questions/1096813/how-to-get-uuid-partition-form-a-given-file-path-in-python
        completed_process = subprocess.run(
            ['/usr/bin/bash', '-c', f'/usr/sbin/blkid -o value $(/usr/bin/df --output=source {shlex.quote(path)} | tail -1) | head -1'],
            check=True,
            text=True,
            capture_output=True
        )
        result = completed_process.stdout.rstrip()
    elif platform.system() == 'Windows':
        command_string = f"$driveLetter = [System.IO.Path]::GetPathRoot('{shlex.quote(path)}').Split(':')[0]; (Get-Partition -DriveLetter $driveLetter).Guid"
        completed_process = subprocess.run(
            [r'powershell.exe', r'-Command', command_string],
            check=True,
            text=True,
            capture_output=True
        )
        #logger.info(f'get_volname({os.getpid()}): raw output is {completed_process.stdout}')
        result = completed_process.stdout.strip('{}\n')
    else:
        raise Exception(f"unsupported system ({platform.system()}), can't get volume name")

    #logger.info(f'get_volname({os.getpid()}): result is {result}')
    return result

def upsert_file(conn, path, statinfo, hash, mimetype):
    logger = logging.getLogger(__name__)
    logger.info(f'upsert_file({os.getpid()}): upserting file with hash {hash} and type {mimetype} - {path}')

    agent_id = os.getpid()

    mime_type, mime_subtype = mimetype.split('/')

    args = [
        str(agent_id),
        platform.node(),
        #get_drivename(path),
        get_volname(path),
        os.path.dirname(path),
        os.path.sep,
        os.path.basename(path),
        mime_type,
        mime_subtype,
        str(statinfo.st_size),
        hash,
        hash_type,
        time.ctime(statinfo.st_ctime),
        time.ctime(statinfo.st_mtime),
        time.ctime(statinfo.st_atime),
        time.ctime()
    ]
    logger.info(f"upsert_file() args: [{', '.join([str(arg) for arg in args])}]")

    result = None
    with conn.cursor() as cursor:
        try:
            cursor.callproc('upsert_file', args)
            result = cursor.fetchone()[0]
            logger.info(f"upsert_file() result is {result}")
            conn.commit()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            conn.rollback()
            raise

    return result

def get_tags(path, image):
    logger = logging.getLogger(__name__)
    logger.debug(f'get_tags({os.getpid()}): starting...')
    tags = {}
    exifdata = image.getexif()

    # now, handle the rest
    for tag_id in exifdata:
        tag = TAGS.get(tag_id, tag_id)
        if tag == tag_id:
            logger.warning(f'unrecognized tag: {tag}')

        if tag_id in EXCLUDED_TAGS or tag in EXCLUDED_TAGS:
            logger.info(f'get_tags({os.getpid()}):   - excluding tag: {tag}')
            continue

        data = exifdata.get(tag_id)

        # handle gps data
        # see https://gist.github.com/erans/983821/e30bd051e1b1ae3cb07650f24184aa15c0037ce8
        # see https://sylvaindurand.org/gps-data-from-photos-with-python/
        if tag == 'GPSInfo':

            logger.info(f'get_tags({os.getpid()}):   - found GPSInfo tag')

            logger.info(f'get_tags({os.getpid()}):   - processing GPSInfo data')

            for gps_tag_id, gps_value in data.items():
                gps_tag = GPSTAGS.get(gps_tag_id, gps_tag_id)

                logger.info(f'get_tags({os.getpid()}):   - found GPSInfo tag: {gps_tag}, with value {gps_value}.')

                if isinstance(gps_value, IFDRational):
                    assert(gps_value.imag == 0)
                    gps_value = gps_value.real
                elif gps_tag in TUPLE_TAGS:
                    ba = bytearray(gps_value)
                    gps_value = tuple(ba)
                elif gps_tag in INT_TAGS:
                    gps_value = int.from_bytes(gps_value, sys.byteorder)
                elif isinstance(gps_value, bytes):
                    gps_value = gps_value.decode()

                if not isinstance(gps_value, str):
                    gps_value = str(gps_value)

                # trim strings from first null code
                try:
                    idx = gps_value.index('\x00')
                    before = gps_value
                    gps_value = gps_value[:idx]
                    logger.debug(f'get_tags({os.getpid()}): HERE 1 - gps_tag is {gps_tag}, idx is {idx}, before is {before}, after is {gps_value}')
                except ValueError:
                    pass

                assert(gps_tag not in tags)
                tags[gps_tag] = gps_value

                logger.info(f'get_tags({os.getpid()}): {gps_tag}: {gps_value}')
        else:
            if isinstance(data, IFDRational):
                assert(data.imag == 0)
                data = data.real
            elif tag in TUPLE_TAGS:
                ba = bytearray(data)
                data = tuple(ba)
            elif tag in INT_TAGS:
                data = int.from_bytes(data, sys.byteorder)
            elif isinstance(data, bytes):
                data = data.decode()

            if not isinstance(data, str):
                data = str(data)

            # trim strings from first null value
            try:
                idx = data.index('\x00')
                before = data
                data = data[:idx]
                logger.debug(f'get_tags({os.getpid()}): HERE 0 - tag is {tag}, idx is {idx}, before is {before}, after is {data}')
            except ValueError:
                pass

            logger.info(f'get_tags({os.getpid()}): {tag}: {data}')
            assert(tag not in tags)
            tags[tag] = data

    logger.debug(f'get_tags({os.getpid()}): done.')

    return tags

def process_image(conn, path, file_id, mimetype):
    logger = logging.getLogger(__name__)
    logger.info(f'process_image({os.getpid()}): processing image file: {path} ({mimetype})')

    image_id = None
    try:
        with Image.open(path) as image:
            # capture (and filter) the image tags, for upload
            tags = get_tags(path, image)
    except Exception as error:
        logger.warning(f'get_tags({os.getpid()}): failed to open image file, skipping...')
        logger.exception(error)
    else:
        # upsert image in db
        image_id = upsert_image(conn, path, file_id, tags)

    # return db image id
    return image_id

def process_file(conn, path, statinfo):
    logger = logging.getLogger(__name__)

    file_size = statinfo.st_size
    if file_size > FILE_SIZE_THRESHOLD:
        logger.warning(f'process_file({os.getpid()}): skipping too-big file: {path}')
        return
    logger.info(f'process_file({os.getpid()}): upserting file of size {file_size}: {path}')

    logger.debug(f'process_file({os.getpid()}): hashing file...')
    hash = hasher.hash_file(path)
    logger.debug(f'process_file({os.getpid()}): hashing complete.')

    mimetype = mime.from_file(path)
    logger.debug(f'process_file({os.getpid()}): mimetype is {mimetype}')

    # upsert file
    file_id = upsert_file(conn, path, statinfo, hash, mimetype)
    logger.info(f'process_file({os.getpid()}): upserted file: {file_id}')

    image_id = None
    if (mimetype.split('/'))[0] == 'image':
        image_id = process_image(conn, path, file_id, mimetype)
    #else:
    #    logger.info(f'process_file({os.getpid()}): skipping file: {path} ({mimetype})')

def scan(workq, idle_worker_count, log_dir):
    pid = os.getpid()

    log_file = os.path.join(log_dir, str(os.getpid()))
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format=LOG_FORMAT
    )
    logger = logging.getLogger(__name__)

    sys.excepthook = exception_handler

    conn = None
    error = None
    try:
        conn = psycopg2.connect(**connection_parameters)

        while True:
            if worker_count == 1 and workq.qsize() == 0:
                logger.info(f'scan({pid}): single-threaded and work queue is empty, quitting...')
                break

            logger.info(f'scan({pid}): fetching new item...')
            item = workq.get()

            if item is None:
                logger.info(f'scan({pid}): found end-of-queue, quitting...')
                break

            with idle_worker_count.get_lock():
                idle_worker_count.value -= 1

            logger.info(f"scan({pid}): dequeued item '{item}'")
            printed_banner = False
            try:
                # assume item is a directory and try scanning it
                for entry in os.scandir(item):
                    if not printed_banner:
                        logger.info(f'scan({pid}): scan: processing directory: {item}')
                        printed_banner = True

                    if excluded(os.path.basename(entry.path)):
                        logger.info(f'scan({pid}): scan: skipping excluded path: {item}')
                        raise ExcludedFile()

                    if entry.is_file(follow_symlinks=False):
                        logger.info(f"scan({pid}): enqueueing item '{entry.path}'")
                        workq.put(entry.path)
                    elif entry.is_dir(follow_symlinks=False):
                        logger.info(f"scan({pid}): enqueueing item '{entry.path}'")
                        workq.put(entry.path)
                    else:
                        logger.info(f'scan({pid}): scan: {entry.path} - not a file or dir, skipping...')
            except ExcludedFile:
                # nothing to do, just continue (with finally block)
                pass
            except NotADirectoryError:
                # is current item a file?
                statinfo = os.stat(item)
                if stat.S_ISREG(statinfo.st_mode):
                    process_file(conn, item, statinfo)
                else:
                    #workq.task_done()
                    #with idle_worker_count.get_lock():
                    #    idle_worker_count.value += 1

                    raise InternalError('Internal error: unknown item type found in work queue')
            finally:
                workq.task_done()
                with idle_worker_count.get_lock():
                    idle_worker_count.value += 1

            #logger.info()

        #logger.info()
        logger.info(f'scan({pid}): scan: at end of loop, workq is {workq.qsize()}, empty = {workq.empty()}')
        #logger.info()
    except(psycopg2.DatabaseError) as error:
        logger.info(f'Database error: {error}')
        if conn:
            conn.rollback()
        raise
    except (psycopg2.Error) as error:
        logger.info(f'Error: {error}')
        if conn:
            conn.rollback()
        raise
    except Exception as error:
        logger.info(f'scan({pid}): here, error is {error}')
        if (error):
            logger.info(f'scan({pid}): getting idle lock')
            with idle_worker_count.get_lock():
                idle_worker_count.value += 1
            logger.info(f'scan({pid}): idled and exiting...')
            raise
    finally:
        workq.close()
        if(conn):
            conn.close()

def usage(exit_code, errmsg=None):
    if errmsg:
        logger.info('Error: ' + errmsg, file=sys.stderr)
    logger.info(f'usage: {sys.argv[0]} <worker-count> <target-directory>[,<target-directory>...]', file=sys.stderr)
    exit(exit_code)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage(1)

    try:
        worker_count = int(sys.argv[1])
    except ValueError as error:
        usage(2, 'worker count must be a non-zero positive integer')

    if worker_count < 1:
        usage(3, 'worker count must be a non-zero positive integer')

    # create the work queue
    workq = mp.JoinableQueue()
    idle_worker_count = mp.Value('i', worker_count)

    # prime the work queue with the list of target directories
    for path in sys.argv[2:]:
        workq.put(os.path.abspath(path))

    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0]))),
        'log',
        platform.node(),
        time.strftime('%Y-%m-%d-%H-%M-%S'),
        str(os.getpid())
    )
    os.makedirs(log_dir, mode=0o755)

    pid = os.getpid()

    if worker_count == 1:
        # configure parent logging
        log_file = os.path.join(log_dir, f'MAIN')
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format=LOG_FORMAT
        )
        logger = logging.getLogger(__name__)

        sys.excepthook = exception_handler

        logger.info(f'main({pid}): starting...')

        scan(workq, idle_worker_count, log_dir)
    else:
        with mp.Pool(worker_count, scan, (workq, idle_worker_count, log_dir)) as pool:
            time.sleep(2)

            # configure parent logging
            log_file = os.path.join(log_dir, f'MAIN')
            logging.basicConfig(
                filename=log_file,
                level=logging.DEBUG,
                format=LOG_FORMAT
            )
            logger = logging.getLogger(__name__)

            sys.excepthook = exception_handler

            logger.info(f'main({pid}): starting...')

            try:
                hang_detected = False
                idle_iterations = 0
                while True:
                    logger.info(f'main({pid}): idle_worker_count is {idle_worker_count.value}, workq size is {workq.qsize()}')

                    if (idle_worker_count.value > worker_count):
                        logger.warning(f'main({pid}): idle_worker_count is too big! (fix the bug)')

                    if (idle_worker_count.value >= worker_count):
                        idle_iterations += 1

                        logger.info(f'main({pid}): all idle (qsize is {workq.qsize()}, idle iteration count is {idle_iterations})...')
                        if workq.qsize() == 0:
                            # all idle and nothing left to do -> shut it down
                            logger.info(f'main({pid}): shutting down 0 (qsize is {workq.qsize()})...')
                            for i in range(worker_count):
                                workq.put(None)

                            while workq.qsize() > 0:
                                time.sleep(QUEUE_DRAIN_SLEEP_INTERVAL)

                            logger.info(f'main({pid}): shutting down 1 (qsize is {workq.qsize()})...')
                            workq.close()

                            logger.info(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                            break
                        elif idle_iterations >= MAX_IDLE_ITERATIONS:
                            # all idle and no progress -> shut it down
                            hang_detected = True
                            logger.debug(f'main({pid}): hang detected, shutting down (qsize is {workq.qsize()})...')

                            for i in range(worker_count):
                                workq.put(None)

                            time.sleep(QUEUE_DRAIN_SLEEP_INTERVAL)

                            logger.debug(f'main({pid}): shutting down 1 (qsize is {workq.qsize()})...')
                            workq.close()

                            logger.debug(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                            break
                    else:
                        idle_iterations = 0

                        logger.info(f'main({pid}): sleeping...')
                        time.sleep(MAIN_LOOP_SLEEP_INTERVAL)
            except Exception as error:
                logger.exception(error)
            finally:
                logger.debug(f'main({pid}): shutting down 3...')

                if hang_detected:
                    logger.error('Error: all idle and no progress, giving up')

    logger.info(f'main({pid}): all done.')

