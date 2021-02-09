
--
-- multipass exec deduplifier-host -- docker exec -it deduplifier_db_1 su - postgres -c 'psql -d deduplifier -h localhost -p 3368 -f /app/code/sql/init.sql'
--

-- remove this stuff so this script can be run against more than one db (e.g. dd_test0)
--DROP DATABASE IF EXISTS deduplifier;
--CREATE DATABASE deduplifier;
--\connect deduplifier;

CREATE TABLE IF NOT EXISTS host (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(253) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

--CREATE TABLE IF NOT EXISTS drive (
--    id INTEGER GENERATED ALWAYS AS IDENTITY,
--    serialno VARCHAR(64) NOT NULL UNIQUE,
--    PRIMARY KEY(id)
--);

CREATE TABLE IF NOT EXISTS volume (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    uuid text NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

-- for background on various ways of representing/managing hierarchical data:
--    https://schinckel.net/2014/11/27/postgres-tree-shootout-part-2%3A-adjacency-list-using-ctes/
--    https://bitworks.software/en/2017-10-20-storing-trees-in-rdbms.html
--    https://www.postgresqltutorial.com/postgresql-self-join/
--    https://stackoverflow.com/questions/47341764/self-referencing-table-sql-query
--    https://persagen.com/2018/06/06/postgresql_trees_recursive_cte.html
CREATE TABLE IF NOT EXISTS path (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    parent_path_id INTEGER REFERENCES path,
    name text NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(parent_path_id, name)
);
-- initialize with bogus root node (to avoid headaches associated with using NULL values)
-- for more info about NULL values complications, see:
--    https://dba.stackexchange.com/questions/151431/postgresql-upsert-issue-with-null-values
--    https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql/42217872#42217872
--    https://stackoverflow.com/questions/34708509/how-to-use-returning-with-on-conflict-in-postgresql/42217872#42217872
--    https://stackoverflow.com/questions/35949877/how-to-include-excluded-rows-in-returning-from-insert-on-conflict/35953488#35953488
INSERT INTO path OVERRIDING SYSTEM VALUE VALUES (0, NULL, '') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
CREATE INDEX IF NOT EXISTS path_parent_path_id_idx ON path (parent_path_id);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    host_id INTEGER NOT NULL,
--    drive_id INTEGER NOT NULL,
    volume_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    sepchar text,
    PRIMARY KEY(id),
    CONSTRAINT fk_host FOREIGN KEY(host_id) REFERENCES host(id) ON DELETE CASCADE,
--    CONSTRAINT fk_drive FOREIGN KEY(drive_id) REFERENCES drive(id) ON DELETE CASCADE,
    CONSTRAINT fk_volume FOREIGN KEY(volume_id) REFERENCES volume(id) ON DELETE CASCADE,
    CONSTRAINT fk_path FOREIGN KEY(path_id) REFERENCES path(id) ON DELETE CASCADE,
--    UNIQUE(host_id, drive_id, volume_id, path_id)
    UNIQUE(host_id, volume_id, path_id)
);
CREATE INDEX IF NOT EXISTS location_host_id_idx ON location (host_id);
CREATE INDEX IF NOT EXISTS location_path_id_idx ON location (path_id);
CREATE INDEX IF NOT EXISTS location_volume_id_idx ON location (volume_id);

CREATE TABLE IF NOT EXISTS file (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    location_id INTEGER NOT NULL,
    name VARCHAR(1024) NOT NULL,
    mime_type TEXT,
    mime_subtype TEXT,
    size_in_bytes INTEGER,
    checksum TEXT,
    checksum_type TEXT,
    create_date DATE NOT NULL,
    modify_date DATE NOT NULL,
    access_date DATE NOT NULL,
    discover_date DATE NOT NULL,
    agent_pid INTEGER NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id) ON DELETE CASCADE,
    UNIQUE(location_id, name)
);
CREATE INDEX IF NOT EXISTS file_location_id_idx ON file (location_id);

CREATE TABLE IF NOT EXISTS exif_tag (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS image (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id INTEGER NOT NULL,
    --imagehash_fingerprint VARCHAR(1024) UNIQUE,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id) ON DELETE CASCADE,
    --UNIQUE(file_id, imagehash_fingerprint)
    UNIQUE(file_id)
);
CREATE INDEX IF NOT EXISTS image_file_id_idx ON image (file_id);


CREATE TABLE IF NOT EXISTS image_tag (
    exif_tag_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    value TEXT NOT NULL,
    CONSTRAINT fk_exif_tag FOREIGN KEY(exif_tag_id) REFERENCES exif_tag(id) ON DELETE CASCADE,
    CONSTRAINT fk_image FOREIGN KEY(image_id) REFERENCES image(id) ON DELETE CASCADE,
    UNIQUE(exif_tag_id, image_id)
);
CREATE INDEX IF NOT EXISTS image_tag_exif_tag_id_idx ON image_tag (exif_tag_id);
CREATE INDEX IF NOT EXISTS image_tag_image_id_idx ON image_tag (image_id);

-- describe tables, views and sequences
\d

