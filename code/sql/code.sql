
CREATE OR REPLACE FUNCTION
add_file(
    agent text,
    hostname text,
    drivename text,
    volumename text,
    filepath text,
    sepchar text,
    filename text,
    checksum text,
    checksum_type text,
    create_date timestamp with time zone,
    modify_date timestamp with time zone,
    access_date timestamp with time zone,
    discover_date timestamp with time zone
)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    host_id INTEGER;
    drive_id INTEGER;
    volume_id INTEGER;
    path_id INTEGER;
    location_id INTEGER;
    file_id INTEGER;
BEGIN
    INSERT INTO host   VALUES(DEFAULT, hostname)   ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO host_id;
    INSERT INTO drive  VALUES(DEFAULT, drivename)  ON CONFLICT (serialno) DO UPDATE SET serialno = EXCLUDED.serialno RETURNING id INTO drive_id;
    INSERT INTO volume VALUES(DEFAULT, volumename) ON CONFLICT (uuid) DO UPDATE SET uuid = EXCLUDED.uuid RETURNING id INTO volume_id;

    path_id := upsert_path(filepath, sepchar);

    INSERT INTO location VALUES(DEFAULT, host_id, drive_id, volume_id, path_id) RETURNING id INTO location_id;

    INSERT INTO file VALUES(DEFAULT, location_id, filename, checksum, checksum_type, create_date, modify_date, access_date, discover_date) RETURNING id INTO file_id;

    RETURN file_id;
END;
$$;

CREATE OR REPLACE FUNCTION
upsert_path(
    remaining_path text,
    sepchar text
)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    insert_string TEXT;
    pos INTEGER;
    parent_id INTEGER;
    path_id INTEGER;
BEGIN
    raise warning 'THIS CODE DOES NOT WORK PROPERLY AS-iS...RETAINED JUST FOR DOC PURPOSES.';
    raise notice 'insert_string is %, remaining_path is %', insert_string, remaining_path;
    WHILE (length(remaining_path) > 0) LOOP
        pos := position(sepchar IN remaining_path);
        --raise notice 'pos is %', pos;
        IF (pos = 0) THEN
            insert_string := remaining_path;
            remaining_path := NULL;
        ELSE
            insert_string := substring(remaining_path for pos-1);
            remaining_path := substring(remaining_path from pos+1);
        END IF;

        IF (length(insert_string) > 0) THEN
            raise notice 'parent_id is %, insert_string is %, remaining_path is %', insert_string, remaining_path, parent_id;

            --INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (parent_path_id, name) WHERE parent_path_id IS NULL DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;
            --INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (name) WHERE parent_path_id IS NULL DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;
            --INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (parent_path_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;
            --INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (parent_path_id, name) WHERE parent_path_id IS NULL DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;
            INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (parent_path_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;

            raise notice '  - INSERT returned path_id %', path_id;
            parent_id := path_id;
        ELSE
            raise notice 'insert_string is empty, looping...';
        END IF;
    END LOOP;
    raise notice '=> returning path_id %', path_id;
    RETURN path_id;
END;
$$;

-- from https://stackoverflow.com/questions/7624919/check-if-a-user-defined-type-already-exists-in-postgresql
DO $$ BEGIN
    CREATE TYPE image_data AS (
        image_id integer,
        image_file_id integer
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION
add_image(
    file_id integer,
    imagehash_fingerprint text
)
RETURNS RECORD LANGUAGE plpgsql AS $$
DECLARE
    image_id INTEGER;
    image_file_id INTEGER;
    retval image_data;
BEGIN
    INSERT INTO image VALUES(DEFAULT, imagehash_fingerprint) ON CONFLICT (name) DO UPDATE SET imagehash_fingerprint = EXCLUDED.imagehash_fingerprint RETURNING id INTO image_id;

    INSERT INTO image_file VALUES(DEFAULT, file_id, image_id) RETURNING id INTO image_file_id;

    retval.image_id := image_id;
    retval.image_file_id := image_file_id;

    RETURN retval;
END;
$$;

