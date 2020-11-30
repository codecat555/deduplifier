
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
    v_host_id INTEGER;
    v_drive_id INTEGER;
    v_volume_id INTEGER;
    v_path_id INTEGER;
    v_location_id INTEGER;
    v_file_id INTEGER;
BEGIN
    INSERT INTO host   VALUES(DEFAULT, hostname)   ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO v_host_id;
    INSERT INTO drive  VALUES(DEFAULT, drivename)  ON CONFLICT (serialno) DO UPDATE SET serialno = EXCLUDED.serialno RETURNING id INTO v_drive_id;
    INSERT INTO volume VALUES(DEFAULT, volumename) ON CONFLICT (uuid) DO UPDATE SET uuid = EXCLUDED.uuid RETURNING id INTO v_volume_id;

    v_path_id := upsert_path(filepath, sepchar);

    --INSERT INTO location VALUES(DEFAULT, v_host_id, v_drive_id, v_volume_id, v_path_id) RETURNING id INTO v_location_id;
    INSERT INTO location VALUES(DEFAULT, v_host_id, v_drive_id, v_volume_id, v_path_id) ON CONFLICT (host_id, drive_id, volume_id, path_id) DO UPDATE SET host_id = EXCLUDED.host_id RETURNING id INTO v_location_id;

    INSERT INTO file VALUES(DEFAULT, v_location_id, filename, checksum, checksum_type, create_date, modify_date, access_date, discover_date) RETURNING id INTO v_file_id;

    RETURN v_file_id;
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
    parent_id INTEGER := 0;
    path_id INTEGER;
BEGIN
    -- remove inital separator
    remaining_path := TRIM(LEADING sepchar FROM remaining_path);
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

        raise notice 'parent_id is %, insert_string is %, remaining_path is %', parent_id, insert_string, remaining_path;

        INSERT INTO path VALUES(DEFAULT, parent_id, insert_string) ON CONFLICT (parent_path_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO path_id;

        raise notice '  - INSERT returned path_id %', path_id;
        parent_id := path_id;
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

