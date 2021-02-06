
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

----delete from file where id in (select f.id from file f join location l on f.location_id = l.id join path         p on l.path_id = p.id where p.id in (select path_id from paths where fullpath like '/mnt/ultra/backup/201602160-partial/me/AppData/%'));
--
---- get all "full" paths, or "file paths" - absolute paths to a file
---- WIP - rename this to file_paths, or just get rid of it and filter all_subpaths when needed?
--DROP MATERIALIZED VIEW IF EXISTS paths CASCADE;
--CREATE MATERIALIZED VIEW paths AS
--WITH RECURSIVE
--allpaths AS (
--    SELECT p.id, p.parent_path_id, p.name AS fullpath, l.sepchar
--    FROM path p
--    -- select only paths which appear in location table, these are "file paths" - i.e. paths that reference files rather than just sub-dirs.
--    JOIN location l ON l.path_id = p.id
--    UNION ALL
--    -- now, climb up the parent hierarchy
--    SELECT child.id, parent.parent_path_id, concat_ws(child.sepchar, parent.name, child.fullpath) AS fullpath, child.sepchar FROM path parent
--    JOIN allpaths child ON parent.id = child.parent_path_id
--)
---- WIP: have to uniqify path id here to avoid over-selection.
--SELECT distinct id path_id,
--    -- fix-up path leading sepchar
--    CASE
--    WHEN sepchar = '/' THEN sepchar || fullpath
--    ELSE fullpath
--    END
---- WIP: use line below to see the inefficiency (over-selection) of this query.
----FROM allpaths;
---- filter out entries which don't go all the way up the tree
--FROM allpaths where parent_path_id = 0;
--
----DROP MATERIALIZED VIEW IF EXISTS all_subpaths_old CASCADE;
----CREATE MATERIALIZED VIEW all_subpaths_old AS
----WITH RECURSIVE
----sepchar_by_file_path AS (
----    WITH not_parent_paths as (
----        -- WIP: why does this return so many (too many) rows?
----        --select id from path p1
----        --where id != ANY(select distinct parent_path_id from path)
----        -- WIP: this is better
----        select id from path
----        EXCEPT select distinct parent_path_id from path
----    )
----    SELECT p.id path_id, l.sepchar
----    FROM not_parent_paths p
----    JOIN location l ON l.path_id = p.id
----),
----allpaths AS (
----    -- start at the top of the tree
----    SELECT p.id path_id, p.parent_path_id, ARRAY[p.name] AS fullpath
----    FROM path p
----    WHERE p.parent_path_id = 0
----  UNION ALL
----    -- explore all subpaths
----    SELECT
----        child.id path_id, child.parent_path_id,
----        array_append(parent.fullpath, child.name) AS fullpath
----    FROM path child
----    JOIN allpaths parent
----    ON child.parent_path_id = parent.path_id
----)
----SELECT ap.path_id, ap.parent_path_id,
----    -- fix-up path leading sepchar
----    array_to_string(
----        CASE
----        WHEN sbfp.sepchar = '/'
----        -- prepend empty string to get leading '/' for unix paths
----        THEN array_prepend('', ap.fullpath)
----        ELSE ap.fullpath
----        END,
----        sbfp.sepchar
----    ) AS fullpath
----FROM allpaths ap
----JOIN sepchar_by_file_path sbfp on ap.path_id = sbfp.path_id
----ORDER BY ap.fullpath asc
----;
--
--DROP MATERIALIZED VIEW IF EXISTS all_subpaths CASCADE;
--CREATE MATERIALIZED VIEW all_subpaths AS
--WITH RECURSIVE
--not_parent_paths as (
--    -- WIP: why does this return so many (too many) rows?
--    --select id from path p1
--    --where id != ANY(select distinct parent_path_id from path)
--    -- WIP: this is better
--    select id from path
--    EXCEPT select distinct parent_path_id from path
--),
--npp_parents as (
--    select p.id, p.parent_path_id from path p
--    JOIN not_parent_paths npp ON p.id = npp.id
--),
--sepchar_by_npp AS (
--    SELECT np.id path_id, np.parent_path_id, l.sepchar
--    FROM npp_parents np
--    JOIN location l ON l.path_id = np.id
--),
--sepchar_for_all_paths AS (
--    -- start at the bottom of the tree
--    SELECT path_id, parent_path_id, sepchar
--    FROM sepchar_by_npp
--    UNION
--    -- explore all parent paths
--    SELECT parent.id path_id, parent.parent_path_id, child.sepchar
--    FROM path parent
--    JOIN sepchar_for_all_paths child ON child.parent_path_id = parent.id
--),
--allpaths AS (
--    -- start at the top of the tree
--    SELECT p.id path_id, p.parent_path_id, ARRAY[p.name] AS fullpath
--    FROM path p
--    WHERE p.parent_path_id = 0
--  UNION
--    -- explore all subpaths
--    SELECT
--        child.id path_id, child.parent_path_id,
--        array_append(parent.fullpath, child.name) AS fullpath
--    FROM path child
--    JOIN allpaths parent
--    ON child.parent_path_id = parent.path_id
--)
--SELECT ap.path_id, ap.parent_path_id,
--    array_to_string(
--        CASE
--        WHEN sfap.sepchar = '/'
--        -- prepend empty string to get leading '/' for unix paths
--        THEN array_prepend('', ap.fullpath)
--        ELSE ap.fullpath
--        END,
--        sfap.sepchar
--    ) AS fullpath
--FROM allpaths ap
--JOIN sepchar_for_all_paths sfap on ap.path_id = sfap.path_id
--;
--
---- this view considers only leaf path nodes, i.e. those that have
---- no children. note that this is not the same as the set of nodes
---- which are referenced by the location table...because some path
---- nodes (many of them, in fact) refer to directories which contain
---- both files and sub-directories.
--DROP MATERIALIZED VIEW IF EXISTS file_dup_groups CASCADE;
--CREATE MATERIALIZED VIEW file_dup_groups AS
---- WIP: later, might simplify dependent queries by including path id in accumulator array, like this:
---- WIP:     array_agg(ARRAY[f.id, p.id])
--SELECT checksum, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.id) file_ids
--FROM file f
--JOIN location l ON f.location_id = l.id
--JOIN path p on l.path_id = p.id
--GROUP BY (f.checksum)
--;
--
--DROP MATERIALIZED VIEW IF EXISTS unique_files CASCADE;
--CREATE MATERIALIZED VIEW unique_files AS
--SELECT file_ids FROM file_dup_groups
--WHERE ARRAY_LENGTH(file_ids, 1) = 1
--;
--
--DROP MATERIALIZED VIEW IF EXISTS duplicate_files CASCADE;
--CREATE MATERIALIZED VIEW duplicate_files AS
--SELECT file_ids FROM file_dup_groups
--WHERE ARRAY_LENGTH(file_ids, 1) > 1
--;
--
--DROP MATERIALIZED VIEW IF EXISTS unique_file_count_for_path CASCADE;
--CREATE MATERIALIZED VIEW unique_file_count_for_path AS
--    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
--    FROM file f
--    JOIN location l ON f.location_id = l.id
--    JOIN path p ON l.path_id = p.id
--    WHERE f.id = ANY(SELECT unnest(file_ids) from unique_files)
--    GROUP BY (p.id)
--;
--
--DROP MATERIALIZED VIEW IF EXISTS duplicate_file_count_for_path CASCADE;
--CREATE MATERIALIZED VIEW duplicate_file_count_for_path AS
--    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
--    FROM file f
--    JOIN location l ON f.location_id = l.id
--    JOIN path p ON l.path_id = p.id
--    WHERE f.id = ANY(SELECT unnest(file_ids) from duplicate_files)
--    GROUP BY (p.id)
--;
--
--DROP MATERIALIZED VIEW IF EXISTS file_dirs_with_totals CASCADE;
--CREATE MATERIALIZED VIEW file_dirs_with_totals AS
--    SELECT
--        p.id path_id,
--        p.parent_path_id,
--        COALESCE(ufc.count, 0) unique_count,
--        COALESCE(dfc.count, 0) duplicate_count,
--        COALESCE(ufc.size_in_bytes, 0) unique_size_in_bytes,
--        COALESCE(dfc.size_in_bytes, 0) dup_size_in_bytes
--    FROM path p
--    LEFT OUTER JOIN unique_file_count_for_path ufc ON p.id = ufc.path_id
--    LEFT OUTER JOIN duplicate_file_count_for_path dfc ON p.id = dfc.path_id
--    WHERE ufc.count > 0 OR dfc.count > 0
--;
--
--DROP MATERIALIZED VIEW IF EXISTS dir_totals_template CASCADE;
--CREATE MATERIALIZED VIEW dir_totals_template AS
--WITH RECURSIVE cte AS (
--    SELECT path_id, parent_path_id, 0 as level, unique_count, duplicate_count, unique_size_in_bytes, dup_size_in_bytes
--    FROM file_dirs_with_totals fdwt
--    UNION
--    SELECT
--        p.id path_id,
--        p.parent_path_id,
--        (dt.level + 1) as level,
--        0 unique_count,
--        0 duplicate_count,
--        0 unique_size_in_bytes,
--        0 dup_size_in_bytes
--    FROM path p
--    JOIN cte dt ON p.id = dt.parent_path_id
--)
--select * from cte
--;
--
--DROP MATERIALIZED VIEW IF EXISTS dir_totals CASCADE;
--CREATE MATERIALIZED VIEW dir_totals AS
--    select
--        parent_path_id path_id,
--        MAX(level) as level,
--        SUM(unique_count) unique_count,
--        SUM(duplicate_count) duplicate_count,
--        SUM(unique_size_in_bytes) unique_size_in_bytes,
--        SUM(dup_size_in_bytes) dup_size_in_bytes
--    from dir_totals_template
--    GROUP BY (parent_path_id)
--    UNION ALL
--    SELECT parent_path_id path_id, 0 as level, unique_count, duplicate_count, unique_size_in_bytes, dup_size_in_bytes
--    FROM file_dirs_with_totals
--;
--
--DROP MATERIALIZED VIEW IF EXISTS subdir_counts_for_path CASCADE;
--CREATE MATERIALIZED VIEW subdir_counts_for_path AS
--    SELECT parent_path_id path_id, COUNT(*) subdir_count FROM path GROUP BY parent_path_id
--;
--
--DROP MATERIALIZED VIEW IF EXISTS dup_counts_for_path CASCADE;
--CREATE MATERIALIZED VIEW dup_counts_for_path AS
--SELECT
--    dt.*,
--    COALESCE(sc.subdir_count, 0) subdir_count
--    FROM dir_totals dt
--    LEFT OUTER JOIN subdir_counts_for_path sc USING(path_id)
--order by path_id
--;
--
---- WIP - why does this currently return only 14221 rows when path contains 233651 unique path id's and
---- WIP - location contains 190289 distinct path_id's (190291 total)?
----DROP MATERIALIZED VIEW IF EXISTS dup_counts_for_path CASCADE;
----CREATE MATERIALIZED VIEW dup_counts_for_path AS
----WITH RECURSIVE
----unique_files AS (
----    SELECT file_ids FROM file_dup_groups
----    WHERE ARRAY_LENGTH(file_ids, 1) = 1
----),
----duplicate_files as (
----    SELECT file_ids FROM file_dup_groups
----    WHERE ARRAY_LENGTH(file_ids, 1) > 1
----),
----unique_file_count_for_path AS (
----    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
----    FROM file f
----    JOIN location l ON f.location_id = l.id
----    JOIN path p ON l.path_id = p.id
----    WHERE f.id = ANY(SELECT unnest(file_ids) from unique_files)
----    GROUP BY (p.id)
----),
----duplicate_file_count_for_path AS (
----    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
----    FROM file f
----    JOIN location l ON f.location_id = l.id
----    JOIN path p ON l.path_id = p.id
----    WHERE f.id = ANY(SELECT unnest(file_ids) from duplicate_files)
----    GROUP BY (p.id)
----),
----file_dirs_with_totals AS (
----    SELECT
----        p.id path_id,
----        p.parent_path_id,
----        COALESCE(ufc.count, 0) unique_count,
----        COALESCE(dfc.count, 0) duplicate_count,
----        COALESCE(ufc.size_in_bytes, 0) unique_size_in_bytes,
----        COALESCE(dfc.size_in_bytes, 0) dup_size_in_bytes
----    FROM path p
----    LEFT OUTER JOIN unique_file_count_for_path ufc ON p.id = ufc.path_id
----    LEFT OUTER JOIN duplicate_file_count_for_path dfc ON p.id = dfc.path_id
----    WHERE ufc.count > 0 OR dfc.count > 0
----),
----dir_totals_template AS (
----    SELECT path_id, parent_path_id, 0 as level, unique_count, duplicate_count, unique_size_in_bytes, dup_size_in_bytes
----    FROM file_dirs_with_totals fdwt
----    UNION ALL
----    SELECT
----        p.id path_id,
----        p.parent_path_id,
----        (dt.level + 1) as level,
----        0 unique_count,
----        0 duplicate_count,
----        0 unique_size_in_bytes,
----        0 dup_size_in_bytes
----    FROM path p
----    JOIN dir_totals_template dt ON p.id = dt.parent_path_id
----),
----dir_totals AS (
----    select
----        parent_path_id path_id,
----        MAX(level) as level,
----        SUM(unique_count) unique_count,
----        SUM(duplicate_count) duplicate_count,
----        SUM(unique_size_in_bytes) unique_size_in_bytes,
----        SUM(dup_size_in_bytes) dup_size_in_bytes
----    from dir_totals_template
----    GROUP BY (parent_path_id)
----    UNION ALL
----    SELECT * from file_dirs_with_totals
----),
----subdir_counts_for_path AS (
----    SELECT parent_path_id path_id, COUNT(*) subdir_count FROM path GROUP BY parent_path_id
----)
----SELECT
----    dt.*,
----    COALESCE(sc.subdir_count, 0) subdir_count
----    FROM dir_totals dt
----    LEFT OUTER JOIN subdir_counts_for_path sc USING(path_id)
----order by path_id
----;
--
---- WIP: need to verify this
--DROP MATERIALIZED VIEW IF EXISTS dup_detail CASCADE;
--CREATE MATERIALIZED VIEW dup_detail AS
--WITH branch_nodes AS (
--    select f.id file_id, l.id location_id, p.id path_id, f.checksum
--    FROM path p
--    JOIN location l on p.id = l.path_id
--    JOIN file f on l.id = f.location_id
--    where
--    p.id in (select distinct path_id from location)
--),
--dups AS (
--    select checksum, sum(size_in_bytes) dup_disk_usage_agg, count(*)
--    FROM file group by checksum
--    -- NOTE: include rows for unique files here, as an aid in data verification
--    --having count(*) > 1
--)
--select d.count dups, d.dup_disk_usage_agg, bn.*
--FROM branch_nodes bn
--JOIN dups d using(checksum)
--order by d.dup_disk_usage_agg desc
--;
--
---- WIP: this isn't right, but is it worth more work?
---- WIP: can this be tossed...is it implemented elsewhere, or what?
----DROP MATERIALIZED VIEW IF EXISTS dup_counts_by_subpath CASCADE;
----CREATE MATERIALIZED VIEW dup_counts_by_subpath AS
----CREATE MATERIALIZED VIEW dup_counts_by_subpath AS
----WITH RECURSIVE x AS (
----    SELECT path_id, parent_path_id, unique_count, duplicate_count, dup_disk_usage_agg, subdir_count
----    FROM dup_counts_for_path
----    UNION ALL
----    SELECT parent.id path_id, parent.parent_path_id, child.unique_count, child.duplicate_count, child.dup_disk_usage_agg, child.subdir_count
----    FROM path parent
----    JOIN x child ON parent.id = child.parent_path_id
----)
----SELECT * FROM x
----ORDER BY dup_disk_usage_agg desc
----;
----;
--
---- WIP - WTF is going on with this?
----DROP MATERIALIZED VIEW IF EXISTS dup_counts_for_file CASCADE;
----CREATE MATERIALIZED VIEW dup_counts_for_file AS
----WITH
----dup_containing_dirs AS (
----    SELECT distinct path_id
----    FROM dup_counts_for_path
----    WHERE duplicate_count > 1
----),
----files_for_path AS (
----    -- NOTE: dup_disk_usage_agg indicates usage by all copies of each path here.
----    SELECT f.id, COUNT(*) dups, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.checksum ORDER BY f.checksum) checksum_list
----    FROM dup_containing_dirs dd
----    JOIN location l ON dd.path_id = l.path_id
----    JOIN file f ON f.location_id = l.id
----    GROUP BY(dd.path_id)
----)
----SELECT * FROM dup_dirs
----WHERE ARRAY_LENGTH(dup_path_ids, 1) > 1
----ORDER BY dup_disk_usage_agg desc
----;
--
--DROP MATERIALIZED VIEW IF EXISTS exact_dup_dirs CASCADE;
--CREATE MATERIALIZED VIEW exact_dup_dirs AS
--WITH
--duplicate_files as (
--    SELECT file_ids FROM file_dup_groups
--    WHERE ARRAY_LENGTH(file_ids, 1) > 1
--),
--dup_containing_dirs AS (
--    SELECT distinct path_id
--    FROM dup_counts_for_path
--    WHERE duplicate_count > 1
--),
--files_for_path AS (
--    -- NOTE: dup_disk_usage_agg indicates usage by all copies of each path here.
--    SELECT dd.path_id, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.checksum ORDER BY f.checksum) checksum_list
--    FROM dup_containing_dirs dd
--    JOIN location l ON dd.path_id = l.path_id
--    JOIN file f ON f.location_id = l.id
--    GROUP BY(dd.path_id)
--),
--dup_dirs AS (
--    SELECT SUM(dup_disk_usage_agg) dup_disk_usage_agg, array_agg(distinct path_id) dup_path_ids
--    FROM files_for_path
--    GROUP BY(checksum_list)
--)
--SELECT * FROM dup_dirs
--WHERE ARRAY_LENGTH(dup_path_ids, 1) > 1
--ORDER BY dup_disk_usage_agg desc
--;
--
----DROP MATERIALIZED VIEW IF EXISTS super_dirs CASCADE;
----CREATE MATERIALIZED VIEW super_dirs AS
----WITH
----dup_containing_dirs AS (
----    SELECT distinct path_id
----    FROM dup_counts_for_path
----    WHERE duplicate_count > 1
----),
----all_checksums_for_path AS (
----    SELECT dd.path_id, array_agg(f.checksum ORDER BY f.checksum) checksum_list
----    FROM dup_containing_dirs dd
----    JOIN location l ON dd.path_id = l.path_id
----    JOIN file f ON f.location_id = l.id
----    GROUP BY(dd.path_id)
----),
----paths_contained_by_path AS (
----    SELECT p1.path_id path_id, array_agg(distinct p2.path_id) subset_dirs
----    FROM all_checksums_for_path p1, all_checksums_for_path p2
----    WHERE p1.path_id <> p2.path_id
----    -- does p1's file list contain p2's list?
----    AND p1.checksum_list @> p2.checksum_list
----    GROUP BY(p1.path_id)
----)
----SELECT array_prepend(path_id, subset_dirs) AS paths_contained_by_path FROM paths_contained_by_path
----;
--
---- describe tables, views and sequences
--\d
--
