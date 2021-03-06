
CREATE OR REPLACE FUNCTION upsert_file
(
    hostname_in text,
--    drivename_in text,
    volumename_in text,
    dirpath_in text,
    sepchar_in text,
    filename_in text,
    mime_type_in text,
    mime_subtype_in text,
    size_in_bytes_in bigint,
    checksum_in text,
    checksum_type_in text,
    create_date_in timestamp with time zone,
    modify_date_in timestamp with time zone,
    access_date_in timestamp with time zone,
    discover_date_in timestamp with time zone,
    agent_pid_in integer
)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_host_id INTEGER;
--    v_drive_id INTEGER;
    v_volume_id INTEGER;
    v_path_id INTEGER;
    v_location_id INTEGER;
    v_file_id INTEGER;
BEGIN
    INSERT INTO host VALUES(DEFAULT, hostname_in) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO v_host_id;
--    INSERT INTO drive  VALUES(DEFAULT, drivename_in)  ON CONFLICT (serialno) DO UPDATE SET serialno = EXCLUDED.serialno RETURNING id INTO v_drive_id;
    INSERT INTO volume VALUES(DEFAULT, volumename_in) ON CONFLICT (uuid) DO UPDATE SET uuid = EXCLUDED.uuid RETURNING id INTO v_volume_id;

    v_path_id := upsert_path(dirpath_in, sepchar_in);

    --INSERT INTO location VALUES(DEFAULT, v_host_id, v_drive_id, v_volume_id, v_path_id, sepchar_in) ON CONFLICT (host_id, drive_id, volume_id, path_id) DO UPDATE SET host_id = EXCLUDED.host_id RETURNING id INTO v_location_id;
    INSERT INTO location VALUES(DEFAULT, v_host_id, v_volume_id, v_path_id, sepchar_in) ON CONFLICT (host_id, volume_id, path_id) DO UPDATE SET host_id = EXCLUDED.host_id RETURNING id INTO v_location_id;

    INSERT INTO file VALUES(DEFAULT, v_location_id, filename_in, mime_type_in, mime_subtype_in, size_in_bytes_in, checksum_in, checksum_type_in, create_date_in, modify_date_in, access_date_in, discover_date_in, agent_pid_in)  ON CONFLICT (location_id, name) DO UPDATE SET location_id = EXCLUDED.location_id RETURNING id INTO v_file_id;

    RETURN v_file_id;
END $$;

CREATE OR REPLACE FUNCTION upsert_path
(
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
END $$;

---- from https://stackoverflow.com/questions/7624919/check-if-a-user-defined-type-already-exists-in-postgresql
--DO $$ BEGIN
--    CREATE TYPE image_data AS (
--        image_id integer,
--        image_file_id integer
--    );
--EXCEPTION
--    WHEN duplicate_object THEN null;
--END $$;

CREATE OR REPLACE FUNCTION upsert_image
(
    file_id_in integer,
    imghash_in text
)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    img_id INTEGER;
BEGIN
    --INSERT INTO image VALUES(DEFAULT, file_id_in, imghash_in) ON CONFLICT (file_id, imagehash_fingerprint) DO UPDATE SET file_id = EXCLUDED.file_id RETURNING id INTO img_id;
    INSERT INTO image VALUES(DEFAULT, file_id_in) ON CONFLICT (file_id) DO UPDATE SET file_id = EXCLUDED.file_id RETURNING id INTO img_id;

    RETURN img_id;
END $$;

---- from https://stackoverflow.com/questions/7624919/check-if-a-user-defined-type-already-exists-in-postgresql
DO $$ BEGIN
    CREATE TYPE image_tag_type AS (
        tag_name text,
        tag_value text
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION upsert_image_tags
(
    img_id INTEGER,
    tag_list image_tag_type[]
)
RETURNS INTEGER[] LANGUAGE plpgsql AS $$
DECLARE
    tag_id INTEGER;
    retval INTEGER[];
BEGIN
    IF array_length(tag_list, 1) > 0 THEN
        FOR i IN 1 .. array_upper(tag_list, 1)
        LOOP
            INSERT INTO exif_tag VALUES(DEFAULT, tag_list[i].tag_name) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id INTO tag_id;
            
            -- do this in a subsequent loop...to avoid deadlocks (
            --INSERT INTO image_tag VALUES(tag_id, img_id, tag_list[i].tag_value) ON CONFLICT (exif_tag_id, image_id) DO UPDATE SET exif_tag_id = EXCLUDED.exif_tag_id;
            -- might try this, instead...
            INSERT INTO image_tag VALUES(tag_id, img_id, tag_list[i].tag_value) ON CONFLICT (exif_tag_id, image_id) DO UPDATE SET image_id = EXCLUDED.image_id;

            retval = array_append(retval, tag_id);
        END LOOP;
        --FOR i IN 1 .. array_upper(tag_list, 1)
        --LOOP
        --    -- try this here, first
        --    INSERT INTO image_tag VALUES(retval[i], img_id, tag_list[i].tag_value) ON CONFLICT (exif_tag_id, image_id) DO UPDATE SET image_id = EXCLUDED.image_id;
        --END LOOP;
    END IF;

    RETURN retval;
END $$;

CREATE OR REPLACE FUNCTION fetch_file_id
(
    dirpath_in text,
    sepchar_in text,
    filename_in text
)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE
    v_file_id integer;
BEGIN
    raise notice 'filename_in is %, dirpath_in is %', filename_in, dirpath_in;

    SELECT f.id FROM file f
    join location l on f.location_id = l.id
    join paths p on p.id = l.path_id
    WHERE f.name = filename_in AND p.fullpath = dirpath_in
    INTO v_file_id;

    raise notice '=> returning file_id %', v_file_id;

    RETURN v_file_id;
END $$;

DO $$ BEGIN
    CREATE TYPE count_type AS (
        count_name text,
        count_value bigint,
        count_bytes bigint
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION get_totals()
RETURNS count_type LANGUAGE sql AS
$$
    SELECT 'TOTALS', count(*), COALESCE(SUM(size_in_bytes), 0) FROM file;
$$;

CREATE OR REPLACE FUNCTION get_counts()
RETURNS count_type[] LANGUAGE plpgsql AS $$
DECLARE
    -- v_file_count count_type;
    --v_counts integer[];
    -- ct count_type;
    result count_type[];
BEGIN
    result := ARRAY(
        SELECT COUNT(*), COALESCE(SUM(size_in_bytes), 0), COALESCE(checksum,'TOTAL') checksum FROM file
        GROUP BY ROLLUP(checksum)
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
    );

    RETURN result;
END $$;

DO $$ BEGIN
    -- replace this with more general count_type above?
    CREATE TYPE files_with_dups_type AS (
        duplicates bigint,
        total_bytes bigint,
        file_name text
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION files_with_dups
(
    offset_in integer,
    limit_in integer,
    sort_field_in text,
    sort_direction_in text
)
RETURNS SETOF files_with_dups_type LANGUAGE plpgsql AS $$
BEGIN
    raise notice 'files_with_dups: offset=%, limit=%', offset_in, limit_in;

    RETURN QUERY EXECUTE '
        SELECT COUNT(*) "count", COALESCE(SUM(size_in_bytes), 0) "bytes", MIN(f.name) "name"
        FROM file f
        JOIN location l ON f.location_id = l.id
        GROUP BY f.name, f.checksum
        HAVING COUNT(*) > 1
        ORDER BY ' || quote_ident(sort_field_in) || ' ' || sort_direction_in || ' OFFSET $1 LIMIT $2'
        USING offset_in, limit_in
    ;
END $$;

CREATE OR REPLACE FUNCTION path_from_file_id
(
    id_in integer
)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_res text;
BEGIN
    --raise notice 'path_from_file_id: id_in=%', id_in;

    select ps.fullpath || l.sepchar || f.name as name from file f
    join location l on f.location_id = l.id
    join path p on l.path_id = p.id
    join paths ps on p.id = ps.path_id
    where f.id = id_in
    into v_res
    ;

    RETURN v_res;
END $$;

CREATE OR REPLACE FUNCTION path_from_path_id
(
    id_in integer
)
RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    v_res text;
BEGIN
    --raise notice 'path_from_path_id: id_in=%', id_in;

    select ps.fullpath from paths ps
    where path_id = id_in
    into v_res
    ;

    RETURN v_res;
END $$;

CREATE OR REPLACE FUNCTION discover_dup_dirs
(
--    id_in integer
)
RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE
    v_res BOOLEAN;
BEGIN
    raise notice 'discover_dup_dirs: starting...';

    -- find the largest set of duplicate files common to each pair of dup-containing paths
    --   for each file with duplicates:
    --       find all parent paths for each file with duplicates:
    --           group results by parent path list to get list of paths with common duplicates

--    RETURN v_res;
END $$;

DO $$ BEGIN
    CREATE TYPE files_for_path_id_type AS (
        file_id integer,
        location_id integer,
        path_id integer,
        fullpath text
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION files_for_path_id
(
    id_in integer
)
RETURNS SETOF files_for_path_id_type LANGUAGE plpgsql AS $$
BEGIN
    --raise notice 'files_with_dups: offset=%, limit=%', offset_in, limit_in;

    RETURN QUERY
    select f.id, l.id, p.id, path_from_file_id(f.id)
    from file f
    join location l on f.location_id = l.id
    join path p on l.path_id = p.id
    where p.id = id_in;
END $$;

DO $$ BEGIN
    CREATE TYPE files_for_checksum_type AS (
        file_id integer,
        location_id integer,
        path_id integer,
        fullpath text
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION files_for_checksum
(
    checksum_in text
)
RETURNS SETOF files_for_checksum_type LANGUAGE plpgsql AS $$
BEGIN
    --raise notice 'files_with_dups: offset=%, limit=%', offset_in, limit_in;

    RETURN QUERY
    select f.id, l.id, p.id, path_from_file_id(f.id)
    from file f
    join location l on f.location_id = l.id
    join path p on l.path_id = p.id
    where f.checksum = checksum_in;
END $$;

