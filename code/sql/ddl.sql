
--
-- multipass exec deduplifier-host -- docker exec -it deduplifier_db_1 su - postgres -c 'psql -d deduplifier -h localhost -p 3368 -f /app/code/sql/init.sql'
--

DROP DATABASE IF EXISTS deduplifier;
CREATE DATABASE deduplifier;
\connect deduplifier;

CREATE TABLE IF NOT EXISTS host (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(253) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS drive (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    serialno VARCHAR(64) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS volume (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    uuid VARCHAR(40) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS path (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    parent_path_id INTEGER REFERENCES path,
    name VARCHAR(40) NOT NULL,
    PRIMARY KEY(id),
    UNIQUE(parent_path_id, name)
);
-- this index enables the UNIQUE constraint above even when parent_path_id is NULL
--CREATE UNIQUE INDEX path_idx1 ON path(parent_path_id, name) WHERE parent_path_id IS NOT NULL;
--CREATE UNIQUE INDEX path_idx2 ON path(name) WHERE parent_path_id IS NULL;
CREATE UNIQUE INDEX path_idx2 ON path(parent_path_id, name) WHERE parent_path_id IS NULL;
-- initialize with bogus root node (to avoid headaches associated with using NULL values)
INSERT INTO path OVERRIDING SYSTEM VALUE VALUES (0, NULL, '');

CREATE TABLE IF NOT EXISTS location (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    host_id INTEGER NOT NULL,
    drive_id INTEGER NOT NULL,
    volume_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_host FOREIGN KEY(host_id) REFERENCES host(id),
    CONSTRAINT fk_drive FOREIGN KEY(drive_id) REFERENCES drive(id),
    CONSTRAINT fk_volume FOREIGN KEY(volume_id) REFERENCES volume(id),
    CONSTRAINT fk_path FOREIGN KEY(path_id) REFERENCES path(id)
);

CREATE TABLE IF NOT EXISTS file (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    location_id INTEGER NOT NULL,
    name VARCHAR(1024) NOT NULL,
    checksum TEXT,
    checksum_type TEXT,
    create_date DATE NOT NULL,
    modify_date DATE NOT NULL,
    access_date DATE NOT NULL,
    discover_date DATE NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id)
);

CREATE TABLE IF NOT EXISTS image (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    --name VARCHAR(1024) NOT NULL,
    imagehash_fingerprint VARCHAR(1024)
);

CREATE TABLE IF NOT EXISTS image_file (
    file_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id),
    CONSTRAINT fk_image FOREIGN KEY(image_id) REFERENCES image(id)
);

-- describe tables, views and sequences
\d

