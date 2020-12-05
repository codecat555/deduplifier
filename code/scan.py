
import sys
import os
import stat

import multiprocessing as mp

import magic

from PIL import Image
from PIL.ExifTags import TAGS

#import imagehash

from filehash import FileHash

MAX_WORKERS = 1

mime = magic.Magic(mime=True)
hasher = FileHash('sha256')

def upsert_image(path, tags):
    print(f'upsert_image: inserting path with tags: {tags}')
    return 1

def upsert_file(path, hash, mimetype):
    print(f'upsert_file: inserting file with hash: {hash} - {path} ({mimetype})')
    return 1

def upsert_image_file(file_id, image_id):
    print(f'upsert_file: inserting image-file relation between image {image_id} and file {file_id}')
    pass

def process_image_file(path, mimetype):
    print(f'process_image_file: processing image file: {path} ({mimetype})')

    with Image.open(path) as image:
        for (tag,value) in enumerate(image._getexif()):
            print(f'IMAGE-DATA:   {TAGS.get(tag)} = {value}')
        tags = { TAGS.get(tag): value for tag,value in enumerate(image._getexif()) }
        #avg_hash = imagehash.average_hash(image)

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
        print(f'process_file: skipping file: {path} ({mimetype})')

    # upsert file
    file_id = upsert_file(path, hash, mimetype)

    # upsert image-file relation
    if not file_id is None and not image_id is None: 
        upsert_image_file(file_id, image_id)

def scan(workq):
    while not workq.empty():
        item = workq.get()
        printed_banner = False
        try:
            # assume item is a directory and try scanning it
            for entry in os.scandir(item):
                if not printed_banner:
                    print(f'scan: processing directory: {item}')
                    printed_banner = True

                if entry.is_file(follow_symlinks=False):
                    workq.put(entry.path)
                elif entry.is_dir(follow_symlinks=False):
                    workq.put(entry.path)
                else:
                    print(f'scan: {entry.path} - not a file or dir, skipping...')
        except NotADirectoryError:
            # is current item a file?
            if stat.S_ISREG(os.stat(item).st_mode):
                process_file(item)
            else:
                raise Exception('unknown item type found in work queue')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'usage: {sys.argv[0]} <target-directory>[,<target-directory>...]', file=sys.stderr)
        exit(1)

    # create the work queue
    workq = mp.Queue()

    # prime the work queue with the list of target directories
    for path in sys.argv[1:]:
        workq.put(os.path.abspath(path))

    with mp.Pool(MAX_WORKERS) as pool:
        scan(workq)

