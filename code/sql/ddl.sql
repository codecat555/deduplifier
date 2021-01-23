
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
INSERT INTO path OVERRIDING SYSTEM VALUE VALUES (0, NULL, '');

CREATE TABLE IF NOT EXISTS location (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    host_id INTEGER NOT NULL,
--    drive_id INTEGER NOT NULL,
    volume_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    sepchar text,
    PRIMARY KEY(id),
    CONSTRAINT fk_host FOREIGN KEY(host_id) REFERENCES host(id),
--    CONSTRAINT fk_drive FOREIGN KEY(drive_id) REFERENCES drive(id),
    CONSTRAINT fk_volume FOREIGN KEY(volume_id) REFERENCES volume(id),
    CONSTRAINT fk_path FOREIGN KEY(path_id) REFERENCES path(id),
--    UNIQUE(host_id, drive_id, volume_id, path_id)
    UNIQUE(host_id, volume_id, path_id)
);

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
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id),
    UNIQUE(location_id, name)
);

CREATE TABLE IF NOT EXISTS exif_tag (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS image (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id INTEGER NOT NULL,
    --imagehash_fingerprint VARCHAR(1024) UNIQUE,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id),
    --UNIQUE(file_id, imagehash_fingerprint)
    UNIQUE(file_id)
);

CREATE TABLE IF NOT EXISTS image_tag (
    exif_tag_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    value TEXT NOT NULL,
    CONSTRAINT fk_exif_tag FOREIGN KEY(exif_tag_id) REFERENCES exif_tag(id),
    CONSTRAINT fk_image FOREIGN KEY(image_id) REFERENCES image(id),
    UNIQUE(exif_tag_id, image_id)
);

DROP MATERIALIZED VIEW IF EXISTS paths_old;
CREATE MATERIALIZED VIEW paths_old AS
WITH RECURSIVE allpaths AS (
    SELECT id, name AS fullpath FROM path WHERE id = 0
    UNION ALL
    SELECT child.id, concat_ws('/', parent.fullpath, child.name) AS fullpath FROM path child
    JOIN allpaths parent ON parent.id = child.parent_path_id
)
SELECT * FROM allpaths;

DROP MATERIALIZED VIEW IF EXISTS paths;
CREATE MATERIALIZED VIEW paths AS
WITH RECURSIVE
loc as (
    select id,path_id,sepchar from location
),
allpaths AS (
    SELECT p.id, p.parent_path_id, p.name AS fullpath, l.sepchar
    FROM path p
    JOIN loc l ON l.path_id = p.id
    UNION ALL
    SELECT child.id, parent.parent_path_id, concat_ws(child.sepchar, parent.name, child.fullpath) AS fullpath, child.sepchar FROM path parent
    JOIN allpaths child ON parent.id = child.parent_path_id
)
SELECT id path_id,
    CASE
    WHEN sepchar = '/' THEN sepchar || fullpath
    ELSE fullpath
    END
FROM allpaths where parent_path_id = 0;

-- this view considers only leaf path nodes, i.e. those that have
-- no children. note that this is not the same as the set of nodes
-- which are referenced by the location table...because some path
-- nodes (many of them, in fact) refer to directories which contain
-- both files and sub-directories.
DROP MATERIALIZED VIEW IF EXISTS dup_groups_for_leaves;
CREATE MATERIALIZED VIEW dup_groups_for_leaves AS
SELECT array_agg(f.id) file_ids
FROM file f
JOIN location l ON f.location_id = l.id
JOIN path p on l.path_id = p.id
WHERE p.id IN (
    -- this sub-query is the mechanism for selecting just leaf path nodes.
    select p1.id from path p1
    left outer join path p2
    on p1.id = p2.parent_path_id
    where p2.parent_path_id IS NULL
)
GROUP BY (f.checksum)
;

DROP MATERIALIZED VIEW IF EXISTS dup_counts;
CREATE MATERIALIZED VIEW dup_counts AS
WITH RECURSIVE
dup_groups_for_all AS (
    SELECT array_agg(f.id) file_ids
    FROM file f
    JOIN location l ON f.location_id = l.id
    JOIN path p on l.path_id = p.id
    GROUP BY (f.checksum)
),
unique_files AS (
    SELECT file_ids FROM dup_groups_for_all
    WHERE ARRAY_LENGTH(file_ids, 1) = 1
),
duplicate_files as (
    SELECT file_ids FROM dup_groups_for_all
    WHERE ARRAY_LENGTH(file_ids, 1) > 1
),
unique_file_count AS (
    SELECT p.id, COUNT(*)
    FROM file f
    JOIN location l ON f.location_id = l.id
    JOIN path p ON l.path_id = p.id
    WHERE f.id = ANY(SELECT unnest(file_ids) from unique_files)
    GROUP BY (p.id)
),
duplicate_file_count AS (
    SELECT p.id, COUNT(*)
    FROM file f
    JOIN location l ON f.location_id = l.id
    JOIN path p ON l.path_id = p.id
    WHERE f.id = ANY(SELECT unnest(file_ids) from duplicate_files)
    GROUP BY (p.id)
),
file_dirs_with_totals AS (
    SELECT p.id path_id, p.parent_path_id, ufc.count unique_count, dfc.count duplicate_count
    FROM path p
    JOIN unique_file_count ufc ON p.id = ufc.id
    JOIN duplicate_file_count dfc ON p.id = dfc.id
),
dir_totals AS (
    SELECT * FROM file_dirs_with_totals fdwt
    UNION ALL
    SELECT p.id path_id, p.parent_path_id, dt.unique_count unique_count, dt.duplicate_count duplicate_count
    FROM path p
    JOIN dir_totals dt ON p.id = dt.parent_path_id
)
--SELECT *
--FROM dir_totals
SELECT
p.fullpath, SUM(dt.unique_count) unique_count, SUM(dt.duplicate_count) duplicate_count
FROM dir_totals dt
JOIN paths p ON p.path_id = dt.path_id
GROUP BY (p.fullpath)
ORDER BY length(p.fullpath) ASC
;

DROP MATERIALIZED VIEW IF EXISTS dup_detail;
CREATE MATERIALIZED VIEW dup_detail AS
WITH branch_nodes AS (
    select f.id file_id, l.id location_id, p.id path_id, f.checksum from path p
    join location l on p.id = l.path_id
    join file f on l.id = f.location_id
    where
    p.id in (select path_id from location)
),
dups AS (
    select checksum, count(*) from file group by checksum
)
select d.count dups, bn.* from branch_nodes bn
join dups d using(checksum)
order by bn.path_id asc
;

-- describe tables, views and sequences
\d

