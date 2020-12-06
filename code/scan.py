
import sys
import os
import stat
import time

import multiprocessing as mp

import magic

from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

#import imagehash

from filehash import FileHash

WORKER_COUNT = 1
MAIN_LOOP_SLEEP_INTERVAL = 3
QUEUE_DRAIN_SLEEP_INTERVAL = 1
EXCLUDED_TAGS = [ 59932, 59933 ]

mime = magic.Magic(mime=True)
hasher = FileHash('sha256')

def upsert_image(path, tags):
    print(f'upsert_image({os.getpid()}): upserting with tags: {path}')
    return 1

def upsert_file(path, hash, mimetype):
    print(f'upsert_file({os.getpid()}): upserting file with hash: {hash} - {path} ({mimetype})')
    return 1

def upsert_image_file(file_id, image_id):
    print(f'upsert_file({os.getpid()}): upserting image-file relation between image {image_id} and file {file_id}')
    pass

def process_image_file(path, mimetype):
    print(f'({os.getpid()})process_image_file: processing image file: {path} ({mimetype})')

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
                if tag != 'UserComment':
                    print(f'process_image_file({os.getpid()}): {tag}: {data}')

                assert(tag not in tags)
                tags[tag] = data

    # upsert image in db
    image_id = upsert_image(path, tags)

    # return db image id
    return image_id

def process_file(path):
    hash = hasher.hash_file(path)

    mimetype = mime.from_file(path)
    image_id = None
    if (mimetype.split('/'))[0] == 'image':
        image_id = process_image_file(path, mimetype)
    else:
        print(f'process_file({os.getpid()}): skipping file: {path} ({mimetype})')

    # upsert file
    file_id = upsert_file(path, hash, mimetype)

    # upsert image-file relation
    if not file_id is None and not image_id is None: 
        upsert_image_file(file_id, image_id)

def scan(workq, idle_worker_count):
    pid = os.getpid()
    while True:
        print(f'scan({pid}): fetching new item...')

        with idle_worker_count.get_lock():
            idle_worker_count.value += 1

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
            if stat.S_ISREG(os.stat(item).st_mode):
                process_file(item)
            else:
                workq.task_done()
                raise Exception('unknown item type found in work queue')

        workq.task_done()
        print()

    print()
    print(f'scan({pid}): scan: at end of loop, workq is {workq.qsize()}, empty = {workq.empty()}')
    print()

    workq.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} <target-directory>[,<target-directory>...]', file=sys.stderr)
        exit(1)

    # create the work queue
    workq = mp.JoinableQueue()
    idle_worker_count = mp.Value('i', 0)

    # prime the work queue with the list of target directories
    for path in sys.argv[1:]:
        workq.put(os.path.abspath(path))

    pid = os.getpid()
    print(f'main({pid}): starting...')

    with mp.Pool(WORKER_COUNT, scan, (workq, idle_worker_count)) as pool:
        while True:
            print(f'main({pid}): idle_worker_count is {idle_worker_count.value}, workq size is {workq.qsize()}')
            if (idle_worker_count.value == WORKER_COUNT) and (workq.qsize() == 0):
                # shut it down

                print(f'main({pid}): shutting down 0 (qsize is {workq.qsize()})...')
                for i in range(WORKER_COUNT):
                    workq.put(None)

                while workq.qsize() > 0:
                    time.sleep(QUEUE_DRAIN_SLEEP_INTERVAL)

                print(f'main({pid}): shutting down 1 (qsize is {workq.qsize()})...')
                workq.close()

                print(f'main({pid}): shutting down 2 (qsize is {workq.qsize()})...')
                break
            else:
                print(f'main({pid}): sleeping...')
                time.sleep(MAIN_LOOP_SLEEP_INTERVAL)

        print(f'main({pid}): shutting down 3...')

    #scan(workq)

    print(f'main({pid}): all done.')

