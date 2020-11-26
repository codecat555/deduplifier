
CREATE OR REPLACE FUNCTION
add_file(
    agent text,
    hostname text,
    drivename text,
    volumename text,
    filepath text,
    sepchar text,
    filename text,
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

    INSERT INTO file VALUES(DEFAULT, location_id, filename, create_date, modify_date, access_date, discover_date) RETURNING id INTO file_id;

    RETURN file_id;
END;
$$;

CREATE OR REPLACE FUNCTION
upsert_path(
    filepath text,
    sepchar text
)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    subpath TEXT;
    pos INTEGER;
    parent_id INTEGER;
    path_id INTEGER;
BEGIN
    raise notice 'filepath is %, subpath is %', filepath, subpath;
    WHILE (length(filepath) > 0) LOOP
        pos := position(sepchar IN filepath);
        raise notice 'pos is %', pos;
        IF (pos = 0) THEN
            subpath := filepath;
            filepath := NULL;
        ELSE
            subpath := substring(filepath for pos-1);
            filepath := substring(filepath from pos+1);
        END IF;
        IF (length(subpath) > 0) THEN
            INSERT INTO path VALUES(DEFAULT, path_id, subpath) ON CONFLICT (parent_path_id, path) DO UPDATE SET path = EXCLUDED.path RETURNING id, parent_path_id INTO path_id, parent_id;
            raise notice 'filepath is %, subpath is %, path_id is %, parent_id is %', filepath, subpath, path_id, parent_id;
        END IF;
    END LOOP;
    raise notice 'returning path_id %', path_id;
    RETURN path_id;
END;
$$;
