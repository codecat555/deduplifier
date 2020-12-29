
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
# https://stackoverflow.com/questions/39496554/cannot-subclass-multiprocessing-queue-in-python-3-5
import multiprocessing.queues as mpq

import logging
import logging.handlers

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
QUEUE_DRAIN_WAIT_TIME = 40

LOG_FORMAT='%(asctime)s %(levelname)s %(message)s'

ABORT_FILE = 'exit'

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

#worker_count = 0

#skip_known_files = False

#abort_path = None

#logger = None

hash_type = 'sha256'

class InternalError(Exception):
    pass

class ExcludedFile(Exception):
    pass

class FoundExitFlag(Exception):
    pass

class FileProcessor:
    def __init__(self, connection_parameters, hasher, skip_known_files):
        self.connection_parameters = connection_parameters
        self.mime = magic.Magic(mime=True)
        self.hasher = hasher
        self.pid = os.getpid()
        self.skip_known_files = skip_known_files

        # we delay opening a connection until the first time process_file() is called
        self.conn = None

    def upsert_image_tags(self, path, img_id, tags):
        logger = logging.getLogger(__name__)

        logger.info(f'upsert_image_tags({self.pid}): upserting image tags: {path}')

        args = [
            img_id,
            # sort keys here to help prevent db deadlocks
            [(key, value) for key,value in sorted(tags.items(), key=lambda x: x[0])]
        ]
        logger.info(f"upsert_image_tags({self.pid}) args: {[str(arg) for arg in args]}")

        result = None
# WIP - make this a property
        MAX_TRIES = 3
        tries = 0
        while tries < MAX_TRIES:
            try:
                with self.conn.cursor() as cursor:
                    #cursor.callproc('upsert_image_tags', args)
                    sql = bytearray(cursor.mogrify('SELECT upsert_image_tags(%s, %s)', args))
                    logger.debug(f"upsert_image_tags({self.pid}): sql is '{sql}'")
                    assert(sql[-1:] == b')')
                    sql[-1:] = b'::image_tag_type[])'
                    sql = bytes(sql)
                    cursor.execute(sql)
                    result = cursor.fetchall()[0][0]
                    #logger.debug(f"upsert_image_tags({self.pid}) result is {result}")
                    self.conn.commit()
                    break
            except psycopg2.errors.DeadlockDetected as error:
                # not sure why deadlocks are occurring here, but maybe this retry will help.
                # see https://stackoverflow.com/questions/46366324/postgres-deadlocks-on-concurrent-upserts
                # see 
                logger.error(f'upsert_image_tags({self.pid}): Deadlock, rolling back and retrying: {error}')
                self.conn.rollback()
                tries += 1
                time.sleep(1)
            except Exception as error:
                logger.info(f'Error calling db: {error}')
                self.conn.rollback()
                raise

        if tries == MAX_TRIES:
            logger.error(f'upsert_image_tags({self.pid}): upsert failed after {tries} tries.')
        elif tries > 0:
            logger.warning(f'upsert_image_tags({self.pid}): upsert succeeded after {tries} tries.')

        if result:
            assert(len(tags) == len(result))
            result = zip(tags, result)
        return result

    def upsert_image(self, path, file_id, tags):
        logger = logging.getLogger(__name__)
        logger.info(f'upsert_image({self.pid}): upserting image: {path}')

        agent_id = self.pid

# WIP - do something here, or just remove it...?
        # fake imagehash for now
        #imghash = str(time.time())
        imghash = None

        args = [
            file_id,
            imghash
        ]
        logger.info(f"upsert_image({self.pid}) args: {[str(arg) for arg in args]}")

        img_id = None
        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('upsert_image', args)
                img_id = cursor.fetchone()[0]
                logger.info(f"upsert_image({self.pid}) img_id is {img_id}")
                self.conn.commit()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            self.conn.rollback()
            raise

        if len(tags) > 0:
            tag_ids = self.upsert_image_tags(path, img_id, tags)
            logger.info(f"upsert_image({self.pid}) tag_ids are {[t for t in tag_ids]}")

        return img_id

    def fetch_file_id(self, path):
        logger = logging.getLogger(__name__)
        logger.info(f'fetch_file_id({self.pid}): fetching file id: {path}')

        agent_id = self.pid

        args = [
            os.path.dirname(path),
            os.path.sep,
            os.path.basename(path),
        ]
        logger.debug(f"fetch_file_id({self.pid}) args: {[str(arg) for arg in args]}")

        file_id = None
        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('fetch_file_id', args)
                file_id = cursor.fetchone()[0]
                logger.debug(f"fetch_file_id({self.pid}) file_id is {file_id}")
                # no need to save anything during fetch
                self.conn.rollback()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            self.conn.rollback()
            raise

        return file_id

    def upsert_file(self, path, statinfo, hash, hash_type, mimetype):
        logger = logging.getLogger(__name__)
        logger.info(f'upsert_file({self.pid}): upserting file with hash {hash} and type {mimetype} - {path}')

        agent_id = self.pid

        result = None

        try:
            mime_type, mime_subtype, *junk = mimetype.split('/')
        except ValueError as error:
            logger.exception(error)
            return result

        if len(junk) > 0:
            logger.warning(f'upsert_file({self.pid}): mimetype has more than two components, ignoring the remainder')

        args = [
            platform.node(),
            #self.get_drivename(path),
            self.get_volname(path),
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
            time.ctime(),
            agent_id
        ]
        logger.info(f"upsert_file({self.pid}) args: {[str(arg) for arg in args]}")

        try:
            with self.conn.cursor() as cursor:
                cursor.callproc('upsert_file', args)
                result = cursor.fetchone()[0]
                logger.info(f"upsert_file({self.pid}) result is {result}")
                self.conn.commit()
        except Exception as error:
            logger.info(f'Error calling db: {error}')
            self.conn.rollback()
            raise

        return result

    def get_drivename(self, path):
        logger = logging.getLogger(__name__)
        logger.info(f'get_drivename({self.pid}): path is {path}')
        if platform.system() == 'Linux':
            result = 1
        elif platform.system() == 'Windows':
            #assert(wmiobj is not None)
            #raise Exception("Windows support not implemented yet, can't get drive name")
            result = 1
        else:
            raise Exception(f"unsupported system ({platform.system()}), can't get drive name")

        #logger.debug(f'get_drivename({self.pid}): result is {result}')
        return result

    def get_volname(self, path):
        logger = logging.getLogger(__name__)
        #logger.info(f'get_volname({self.pid}): path is {path}')
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
            path = re.sub(r"'", r"''", path)
            command_string = f"$driveLetter = [System.IO.Path]::GetPathRoot('{path}').Split(':')[0]; (Get-Partition -DriveLetter $driveLetter).Guid"
            completed_process = subprocess.run(
                [r'powershell.exe', r'-Command', command_string],
                check=True,
                text=True,
                capture_output=True
            )
            #logger.info(f'get_volname({self.pid}): raw output is {completed_process.stdout}')
            result = completed_process.stdout.strip('{}\n')
        else:
            raise Exception(f"unsupported system ({platform.system()}), can't get volume name")

        #logger.info(f'get_volname({self.pid}): result is {result}')
        return result

    def get_tags(self, path, image):
        logger = logging.getLogger(__name__)
        logger.info(f'get_tags({self.pid}): starting...')
        tags = {}
        exifdata = image.getexif()

        # now, handle the rest
        for tag_id in exifdata:
            tag = TAGS.get(tag_id, tag_id)
            if tag == tag_id:
                logger.warning(f'unrecognized tag: {tag}')

            if tag_id in EXCLUDED_TAGS or tag in EXCLUDED_TAGS:
                logger.info(f'get_tags({self.pid}):   - excluding tag: {tag}')
                continue

            data = exifdata.get(tag_id)

            # handle gps data
            # see https://gist.github.com/erans/983821/e30bd051e1b1ae3cb07650f24184aa15c0037ce8
            # see https://sylvaindurand.org/gps-data-from-photos-with-python/
            if tag == 'GPSInfo':

                logger.info(f'get_tags({self.pid}):   - found GPSInfo tag')

                logger.info(f'get_tags({self.pid}):   - processing GPSInfo data')

                for gps_tag_id, gps_value in data.items():
                    gps_tag = GPSTAGS.get(gps_tag_id, gps_tag_id)

                    logger.info(f'get_tags({self.pid}):   - found GPSInfo tag: {gps_tag}, with value {gps_value}.')

                    if isinstance(gps_value, IFDRational):
                        assert(gps_value.imag == 0)
                        gps_value = gps_value.real
                    elif gps_tag in TUPLE_TAGS:
                        ba = bytearray(gps_value)
                        gps_value = tuple(ba)
                    elif gps_tag in INT_TAGS:
                        gps_value = int.from_bytes(gps_value, sys.byteorder)
                    elif isinstance(gps_value, bytes):
                        try:
                            gps_value = gps_value.decode()
                        except UnicodeDecodeError as error:
                            logger.warning(f'get_tags({self.pid}): failed to decode tag value, skipping... ({gps_tag}: {gps_value})')
                            logger.exception(error)
                            continue

                    if not isinstance(gps_value, str):
                        gps_value = str(gps_value)

                    # trim strings from first null code
                    try:
                        idx = gps_value.index('\x00')
                        before = gps_value
                        gps_value = gps_value[:idx]
# WIP - review this
                        logger.debug(f'get_tags({self.pid}): HERE 1 - gps_tag is {gps_tag}, idx is {idx}, before is {before}, after is {gps_value}')
                    except ValueError:
                        pass

                    assert(gps_tag not in tags)
                    tags[gps_tag] = gps_value

                    logger.info(f'get_tags({self.pid}): {gps_tag}: {gps_value}')
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
                    try:
                        data = data.decode()
                    except UnicodeDecodeError as error:
                        logger.warning(f'get_tags({self.pid}): failed to decode tag value, skipping... ({tag}: {data})')
                        logger.exception(error)
                        continue

                if not isinstance(data, str):
                    data = str(data)

                # trim strings from first null value
                try:
                    idx = data.index('\x00')
                    before = data
                    data = data[:idx]
                    logger.debug(f'get_tags({self.pid}): HERE 0 - tag is {tag}, idx is {idx}, before is {before}, after is {data}')
                except ValueError:
                    pass

                logger.info(f'get_tags({self.pid}): {tag}: {data}')
                assert(tag not in tags)
                tags[tag] = data

        logger.debug(f'get_tags({self.pid}): done.')

        return tags

    def process_image(self, path, file_id, mimetype):
        logger = logging.getLogger(__name__)
        logger.info(f'process_image({self.pid}): processing image file: {path} ({mimetype})')

        image_id = None
        try:
            with Image.open(path) as image:
                # capture (and filter) the image tags, for upload
                tags = self.get_tags(path, image)
        except Exception as error:
            logger.warning(f'get_tags({self.pid}): failed to open image file, skipping...')
            logger.exception(error)
        else:
            # upsert image in db
            image_id = self.upsert_image(path, file_id, tags)

        # return db image id
        return image_id

    def process_file(self, path, statinfo, skip_known_files, hash_type):
        logger = logging.getLogger(__name__)

        if not self.conn:
            self.conn = psycopg2.connect(**self.connection_parameters)

        file_size = statinfo.st_size
# WIP - make this limit a parameter
        if file_size > FILE_SIZE_THRESHOLD:
            logger.warning(f'process_file({self.pid}): skipping too-big file: {path}')
            return

        if skip_known_files:
            file_id = self.fetch_file_id(path)
            if file_id:
                logger.info(f'process_file({self.pid}): file already in db, skipping...: {file_id}')
                return file_id

        logger.info(f'process_file({self.pid}): upserting file of size {file_size}: {path}')

        logger.debug(f'process_file({self.pid}): hashing file...')
        hash = self.hasher.hash_file(path)
        logger.debug(f'process_file({self.pid}): hashing complete.')

        try:
            mimetype = self.mime.from_file(path)
        except magic.magic.MagicException as error:
            logger.error(f'process_file({self.pid}): failed to get mimetype: {error}')
            return None
        logger.debug(f'process_file({self.pid}): mimetype is {mimetype}')

        # upsert file
        file_id = self.upsert_file(path, statinfo, hash, hash_type, mimetype)
        logger.info(f'process_file({self.pid}): upserted file: {file_id}')

        image_id = None
        if (mimetype.split('/'))[0] == 'image':
            image_id = self.process_image(path, file_id, mimetype)
        #else:
        #    logger.info(f'process_file({self.pid}): skipping file: {path} ({mimetype})')

    def __del__(self):
        if self.conn:
            self.conn.close()

def excluded(string):
    for pat in EXCLUDE_REGEX:
        if pat.match(string):
            return True
    return False

def scan(workq, idle_worker_count, log_dir, skip_known_files, worker_count, abort_path, hash_type):
    pid = os.getpid()

    log_file = os.path.join(log_dir, str(os.getpid()))
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format=LOG_FORMAT
    )
    logger = logging.getLogger(__name__)

    logger.info(f'scan({pid}): starting (skip_known_files = {skip_known_files})...')

    fp = None
    error = None
    try:
        hasher = FileHash(hash_type)
        fp = FileProcessor(connection_parameters, hasher, skip_known_files)

        while True:
            # special, single-threaded, case
            if worker_count == 1:
                if workq.qsize() == 0:
                    logger.info(f'scan({pid}): single-threaded and work queue is empty, quitting...')
                    break

                try:
                    os.stat(abort_path)
                except FileNotFoundError:
                    pass
                else:
                    #raise FoundExitFlag()
                    logger.debug(f'scan({pid}): found exit request file, quitting...')
                    break

            logger.info(f'scan({pid}): fetching new item...')
            logger.info(f'scan({pid}): quitting time is {workq._quitting_time}...')
            item = workq.get()

            if item is None:
                logger.info(f'scan({pid}): found end-of-queue, quitting...')
                break
            elif item == -1:
                logger.info(f'scan({pid}): found time-to-quit, quitting...')
                time.sleep(1)
                raise TimeToQuitException1()
                break

            logger.info(f"scan({pid}): dequeued item '{item}'")
            printed_banner = False
            with idle_worker_count.get_lock():
                idle_worker_count.value -= 1
            try:
                # is current item a file?
                statinfo = os.stat(item)
                if stat.S_ISREG(statinfo.st_mode):
                    fp.process_file(item, statinfo, skip_known_files, hash_type)
                elif stat.S_ISDIR(statinfo.st_mode):
                    # a directory, scan it and queue the interesting entries
                    for entry in os.scandir(item):
                        if not printed_banner:
                            logger.info(f'scan({pid}): scan: processing directory: {entry.path}')
                            printed_banner = True

                        if excluded(os.path.basename(entry.path)):
                            logger.info(f'scan({pid}): scan: skipping excluded path: {entry.path}')
                            raise ExcludedFile()

                        if entry.is_file(follow_symlinks=False):
                            logger.info(f"scan({pid}): enqueueing item '{entry.path}'")
                            workq.put(entry.path)
                        elif entry.is_dir(follow_symlinks=False):
                            logger.info(f"scan({pid}): enqueueing item '{entry.path}'")
                            workq.put(entry.path)
                        else:
                            logger.info(f'scan({pid}): scan: {entry.path} - not a file or dir, skipping...')
                else:
                    raise InternalError('Internal error: unknown item type found in work queue')
            except FileNotFoundError:
                logger.error('entry disappeared while sitting in queue')
                logger.exception(error)
            except ExcludedFile:
                # nothing to do, just continue (with finally block)
                pass
            except PermissionError as error:
                # log it and continue
                logger.exception(error)
            finally:
                with idle_worker_count.get_lock():
                    idle_worker_count.value += 1

        logger.info(f'scan({pid}): scan: at end of loop, workq is {workq.qsize()}, empty = {workq.empty()}')

    except TimeToQuitException as error:
        logger.exception(error)
        logger.info(f'scan({pid}): time to quit, idled and exiting...')
        time.sleep(2)
    finally:
        #workq.close()
        pass

def usage(exit_code, errmsg=None):
    if errmsg:
        logger.info('Error: ' + errmsg, file=sys.stderr)
    logger.info(f'usage: {sys.argv[0]} [-skip_known_files] <worker-count> <target-directory>[,<target-directory>...]', file=sys.stderr)
    exit(exit_code)

class TimeToQuitException(Exception):
    pass

class TimeToQuitException1(Exception):
    pass

# a shared queue that can be interrupted
class InterruptibleQueue(mpq.Queue):
    def __init__(self, *args, **kwargs):
        #print(f'InterruptibleQueue::__init__({os.getpid()}): here')
        super().__init__(*args, **kwargs, ctx=mp.get_context())
        self._quitting_time = False

    def get(self, *args, **kwargs):
        if self._quitting_time:
            print(f'InterruptibleQueue::get({os.getpid()}): raising TimeToQuitException')
            #raise TimeToQuitException()
            return -1
        item = super().get(*args, **kwargs)
        #print(f'InterruptibleQueue::get({os.getpid()}): returning {item}')
        return item

    def put(self, *args, **kwargs):
        if self._quitting_time:
            #raise TimeToQuitException()
            return
        #print(f'InterruptibleQueue::put({os.getpid()}): {args, kwargs}')
        super().put(*args, **kwargs)

#    @property
#    def quitting_time(self):
#        return self._quitting_time
#
#    @quitting_time.setter
#    def quitting_time(self, val):
#        self._quitting_time = val

    def time_to_quit(self):
        print(f'{time.asctime()} InterruptibleQueue::get({os.getpid()}): TIME TO QUIT')
        #self.quitting_time(True)
        self._quitting_time = True

    def __getstate__(self):
        # https://stackoverflow.com/questions/52278349/subclassing-multiprocessing-queue-queue-attributes-set-by-parent-not-available
        retval = (self._ignore_epipe, self._maxsize, self._reader, self._writer,
                        self._rlock, self._wlock, self._sem, self._opid,
                                        self._quitting_time)  
        return retval

    def __setstate__(self, state):
        # https://stackoverflow.com/questions/52278349/subclassing-multiprocessing-queue-queue-attributes-set-by-parent-not-available
        (self._ignore_epipe, self._maxsize, self._reader, self._writer,
                 self._rlock, self._wlock, self._sem, self._opid,
                          self._quitting_time) = state
        self._after_fork()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage(1)

    first_arg_idx = 1
    if '-skip-known-files' in sys.argv:
        skip_known_files = True
        first_arg_idx = 2

    worker_count = 0
    try:
        worker_count = int(sys.argv[first_arg_idx])
    except ValueError as error:
        usage(2, 'worker count must be a non-zero positive integer')

    if worker_count < 1:
        usage(3, 'worker count must be a non-zero positive integer')

    # do this before any other mp stuff
    mp.set_start_method('spawn')

    # create the work queue
    workq = InterruptibleQueue()
    #workq = mpq.Queue(ctx=mp.get_context())
    idle_worker_count = mp.Value('i', worker_count)

    # prime the work queue with the list of target directories
    for path in sys.argv[first_arg_idx+1:]:
        workq.put(os.path.abspath(path))

    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(sys.argv[0]))),
        #'/extra/me',
        'log',
        platform.node(),
        time.strftime('%Y-%m-%d-%H-%M-%S'),
        str(os.getpid())
    )
    os.makedirs(log_dir, mode=0o755)
    # print log dir to stdout, for convenience
    print(f"log dir is {log_dir}")

    abort_path = os.path.join(log_dir, ABORT_FILE)

    pid = os.getpid()

    if worker_count == 1:
        # configure parent logging
        log_file = os.path.join(log_dir, f'main')
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format=LOG_FORMAT
        )
        logger = logging.getLogger(__name__)

        logger.info(f'main({pid}): starting...')

        scan(workq, idle_worker_count, log_dir, skip_known_files, worker_count, abort_path, hash_type)
    else:
        with mp.Pool(worker_count, scan, (workq, idle_worker_count, log_dir, skip_known_files, worker_count, abort_path, hash_type)) as pool:
            #time.sleep(10)

            # configure parent logging
            log_file = os.path.join(log_dir, f'main')
            logging.basicConfig(
                filename=log_file,
                level=logging.DEBUG,
                format=LOG_FORMAT
            )
            logger = logging.getLogger(__name__)

            logger.info(f'main({pid}): starting...')
            while mp.active_children() == 0:
                time.sleep(1)
            time.sleep(2)
            #STARTUP_WAIT_TIME = 20
            #time.sleep(STARTUP_WAIT_TIME)

            try:
                hang_detected = False
                idle_iterations = 0
                while True:
                    logger.info(f'main({pid}): idle_worker_count is {idle_worker_count.value}, workq size is {workq.qsize()}')

                    try:
                        os.stat(abort_path)
                    except FileNotFoundError:
                        pass
                    else:
                        workq.time_to_quit()
                        msg = f'NOTICE: found exit request file, quitting in {QUEUE_DRAIN_WAIT_TIME} seconds...'
                        logger.info(msg)
                        print(msg)
                        time.sleep(QUEUE_DRAIN_WAIT_TIME)
                        pool.terminate()
                        pool.join()
                        raise FoundExitFlag()

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

                            logger.info(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                            break
                        elif idle_iterations >= MAX_IDLE_ITERATIONS:
                            # all idle and no progress -> shut it down
                            hang_detected = True
                            logger.debug(f'main({pid}): hang detected, shutting down (qsize is {workq.qsize()})...')

                            for i in range(worker_count):
                                workq.put(None)

                            time.sleep(QUEUE_DRAIN_WAIT_TIME)

                            logger.debug(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')

                            break
                    else:
                        idle_iterations = 0

                        logger.info(f'main({pid}): sleeping...')

                    #time.sleep(MAIN_LOOP_SLEEP_INTERVAL)
            except BaseException as error:
                logger.exception(error)
                pool.terminate()
                pool.join()
            finally:
                logger.debug(f'main({pid}): shutting down 3...')

                if hang_detected:
                    logger.error('Error: all idle and no progress, giving up')

    logger.debug(f'main({pid}): closing workq...')
    workq.close()

    logger.info(f'main({pid}): all done.')

