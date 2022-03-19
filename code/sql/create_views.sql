--delete from file where id in (select f.id from file f join location l on f.location_id = l.id join path         p on l.path_id = p.id where p.id in (select path_id from paths where fullpath like '/mnt/ultra/backup/201602160-partial/me/AppData/%'));

-- get all "full" paths, or "file paths" - absolute paths to a file
-- WIP - rename this to file_paths, or just get rid of it and filter all_subpaths when needed?
DROP MATERIALIZED VIEW IF EXISTS paths CASCADE;
CREATE MATERIALIZED VIEW paths AS
WITH RECURSIVE
allpaths AS (
    SELECT p.id, p.parent_path_id, p.name AS fullpath, l.sepchar
    FROM path p
    -- select only paths which appear in location table, these are "file paths" - i.e. paths that reference files rather than just sub-dirs.
    JOIN location l ON l.path_id = p.id
    UNION ALL
    -- now, climb up the parent hierarchy
    SELECT child.id, parent.parent_path_id, concat_ws(child.sepchar, parent.name, child.fullpath) AS fullpath, child.sepchar FROM path parent
    JOIN allpaths child ON parent.id = child.parent_path_id
)
-- WIP: have to uniqify path id here to avoid over-selection.
SELECT distinct id path_id,
    -- fix-up path leading sepchar
    CASE
    WHEN sepchar = '/' THEN sepchar || fullpath
    ELSE fullpath
    END
-- WIP: use line below to see the inefficiency (over-selection) of this query.
--FROM allpaths;
-- filter out entries which don't go all the way up the tree
FROM allpaths where parent_path_id = 0;

--DROP MATERIALIZED VIEW IF EXISTS all_subpaths_old CASCADE;
--CREATE MATERIALIZED VIEW all_subpaths_old AS
--WITH RECURSIVE
--sepchar_by_file_path AS (
--    WITH not_parent_paths as (
--        -- WIP: why does this return so many (too many) rows?
--        --select id from path p1
--        --where id != ANY(select distinct parent_path_id from path)
--        -- WIP: this is better
--        select id from path
--        EXCEPT select distinct parent_path_id from path
--    )
--    SELECT p.id path_id, l.sepchar
--    FROM not_parent_paths p
--    JOIN location l ON l.path_id = p.id
--),
--allpaths AS (
--    -- start at the top of the tree
--    SELECT p.id path_id, p.parent_path_id, ARRAY[p.name] AS fullpath
--    FROM path p
--    WHERE p.parent_path_id = 0
--  UNION ALL
--    -- explore all subpaths
--    SELECT
--        child.id path_id, child.parent_path_id,
--        array_append(parent.fullpath, child.name) AS fullpath
--    FROM path child
--    JOIN allpaths parent
--    ON child.parent_path_id = parent.path_id
--)
--SELECT ap.path_id, ap.parent_path_id,
--    -- fix-up path leading sepchar
--    array_to_string(
--        CASE
--        WHEN sbfp.sepchar = '/'
--        -- prepend empty string to get leading '/' for unix paths
--        THEN array_prepend('', ap.fullpath)
--        ELSE ap.fullpath
--        END,
--        sbfp.sepchar
--    ) AS fullpath
--FROM allpaths ap
--JOIN sepchar_by_file_path sbfp on ap.path_id = sbfp.path_id
--ORDER BY ap.fullpath asc
--;

DROP MATERIALIZED VIEW IF EXISTS all_subpaths CASCADE;
CREATE MATERIALIZED VIEW all_subpaths AS
WITH RECURSIVE
not_parent_paths as (
    -- WIP: why does this return so many (too many) rows?
    --select id from path p1
    --where id != ANY(select distinct parent_path_id from path)
    -- WIP: this is better
    select id from path
    EXCEPT select distinct parent_path_id from path
),
npp_parents as (
    select p.id, p.parent_path_id from path p
    JOIN not_parent_paths npp ON p.id = npp.id
),
sepchar_by_npp AS (
    SELECT np.id path_id, np.parent_path_id, l.sepchar
    FROM npp_parents np
    JOIN location l ON l.path_id = np.id
),
sepchar_for_all_paths AS (
    -- start at the bottom of the tree
    SELECT path_id, parent_path_id, sepchar
    FROM sepchar_by_npp
    UNION
    -- explore all parent paths
    SELECT parent.id path_id, parent.parent_path_id, child.sepchar
    FROM path parent
    JOIN sepchar_for_all_paths child ON child.parent_path_id = parent.id
),
allpaths AS (
    -- start at the top of the tree
    SELECT p.id path_id, p.parent_path_id, ARRAY[p.name] AS fullpath
    FROM path p
    WHERE p.parent_path_id = 0
  UNION
    -- explore all subpaths
    SELECT
        child.id path_id, child.parent_path_id,
        array_append(parent.fullpath, child.name) AS fullpath
    FROM path child
    JOIN allpaths parent
    ON child.parent_path_id = parent.path_id
)
SELECT ap.path_id, ap.parent_path_id,
    array_to_string(
        CASE
        WHEN sfap.sepchar = '/'
        -- prepend empty string to get leading '/' for unix paths
        THEN array_prepend('', ap.fullpath)
        ELSE ap.fullpath
        END,
        sfap.sepchar
    ) AS fullpath
FROM allpaths ap
JOIN sepchar_for_all_paths sfap on ap.path_id = sfap.path_id
;

-- this view considers only leaf path nodes, i.e. those that have
-- no children. note that this is not the same as the set of nodes
-- which are referenced by the location table...because some path
-- nodes (many of them, in fact) refer to directories which contain
-- both files and sub-directories.
DROP MATERIALIZED VIEW IF EXISTS file_dup_groups CASCADE;
CREATE MATERIALIZED VIEW file_dup_groups AS
-- WIP: later, might simplify dependent queries by including path id in accumulator array, like this:
-- WIP:     array_agg(ARRAY[f.id, p.id])
SELECT checksum, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.id) file_ids
FROM file f
JOIN location l ON f.location_id = l.id
JOIN path p on l.path_id = p.id
GROUP BY (f.checksum)
;

DROP MATERIALIZED VIEW IF EXISTS unique_files CASCADE;
CREATE MATERIALIZED VIEW unique_files AS
SELECT file_ids FROM file_dup_groups
WHERE ARRAY_LENGTH(file_ids, 1) = 1
;

DROP MATERIALIZED VIEW IF EXISTS duplicate_files CASCADE;
CREATE MATERIALIZED VIEW duplicate_files AS
SELECT file_ids FROM file_dup_groups
WHERE ARRAY_LENGTH(file_ids, 1) > 1
;

DROP MATERIALIZED VIEW IF EXISTS unique_file_count_for_path CASCADE;
CREATE MATERIALIZED VIEW unique_file_count_for_path AS
    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
    FROM file f
    JOIN location l ON f.location_id = l.id
    JOIN path p ON l.path_id = p.id
    WHERE f.id = ANY(SELECT unnest(file_ids) from unique_files)
    GROUP BY (p.id)
;

DROP MATERIALIZED VIEW IF EXISTS duplicate_file_count_for_path CASCADE;
CREATE MATERIALIZED VIEW duplicate_file_count_for_path AS
    SELECT p.id path_id, COUNT(*), SUM(f.size_in_bytes) size_in_bytes
    FROM file f
    JOIN location l ON f.location_id = l.id
    JOIN path p ON l.path_id = p.id
    WHERE f.id = ANY(SELECT unnest(file_ids) from duplicate_files)
    GROUP BY (p.id)
;

DROP MATERIALIZED VIEW IF EXISTS file_dirs_with_totals CASCADE;
CREATE MATERIALIZED VIEW file_dirs_with_totals AS
    SELECT
        p.id path_id,
        p.parent_path_id,
        COALESCE(ufc.count, 0) unique_count,
        COALESCE(dfc.count, 0) duplicate_count,
        COALESCE(ufc.size_in_bytes, 0) unique_size_in_bytes,
        COALESCE(dfc.size_in_bytes, 0) dup_size_in_bytes
    FROM path p
    LEFT OUTER JOIN unique_file_count_for_path ufc ON p.id = ufc.path_id
    LEFT OUTER JOIN duplicate_file_count_for_path dfc ON p.id = dfc.path_id
    WHERE ufc.count > 0 OR dfc.count > 0
;

DROP MATERIALIZED VIEW IF EXISTS dir_totals_expansion CASCADE;
CREATE MATERIALIZED VIEW dir_totals_expansion AS
WITH RECURSIVE cte AS (
    -- start with the set of file-containing paths
    SELECT path_id, parent_path_id, NULL::integer child_path_id, 0 as level, unique_count, duplicate_count, unique_size_in_bytes, dup_size_in_bytes
    FROM file_dirs_with_totals fdwt
    UNION
    -- create a row for all ancestors of each file-containing path with that path's values
    SELECT
        p.id path_id,
        p.parent_path_id,
        dt.path_id child_path_id,
        (dt.level + 1) as level,
        dt.unique_count unique_count,
        dt.duplicate_count duplicate_count,
        dt.unique_size_in_bytes unique_size_in_bytes,
        dt.dup_size_in_bytes dup_size_in_bytes
    FROM path p
    JOIN cte dt ON p.id = dt.parent_path_id
)
select * from cte
;

DROP MATERIALIZED VIEW IF EXISTS dir_totals CASCADE;
CREATE MATERIALIZED VIEW dir_totals AS
    select
        path_id,
        parent_path_id,
        MAX(level) as level,
        SUM(unique_count) unique_count,
        SUM(duplicate_count) duplicate_count,
        SUM(unique_size_in_bytes) unique_size_in_bytes,
        SUM(dup_size_in_bytes) dup_size_in_bytes
    from dir_totals_expansion
    GROUP BY (path_id, parent_path_id)
;

DROP MATERIALIZED VIEW IF EXISTS dup_dirs_expansion CASCADE;
CREATE MATERIALIZED VIEW dup_dirs_expansion AS
WITH RECURSIVE
file_containing_paths AS (
    SELECT fdwt.path_id, fdwt.parent_path_id, array_agg(f.checksum ORDER BY f.checksum) checksum_list
    FROM file_dirs_with_totals fdwt
    join location l on l.path_id = fdwt.path_id
    join file f on f.location_id = l.id
    --WHERE fdwt.unique_count = 0 AND fdwt.duplicate_count > 0
    WHERE fdwt.duplicate_count > 0
    GROUP BY(fdwt.path_id, fdwt.parent_path_id)
),
cte AS (
    -- start with the set of file-containing paths
    SELECT path_id, parent_path_id, NULL::integer child_path_id, 0 as level, checksum_list
    FROM file_containing_paths
    UNION
    -- create a row for all ancestors of each file-containing path with that path's values
    SELECT
        parent.id path_id,
        parent.parent_path_id,
        child.path_id child_path_id,
        (child.level + 1) as level,
        checksum_list
    FROM path parent
    JOIN cte child ON parent.id = child.parent_path_id
)
select * from cte
;
CREATE INDEX IF NOT EXISTS dup_dirs_expansion_path_id_idx ON dup_dirs_expansion (path_id);
CREATE INDEX IF NOT EXISTS dup_dirs_expansion_parent_path_id_idx ON dup_dirs_expansion (parent_path_id);
CREATE INDEX IF NOT EXISTS dup_dirs_expansion_level_idx ON dup_dirs_expansion (level);
CREATE INDEX IF NOT EXISTS dup_dirs_expansion_composite_idx ON dup_dirs_expansion (path_id, parent_path_id, level);

--DROP MATERIALIZED VIEW IF EXISTS dup_dirs_contraction CASCADE;
--CREATE MATERIALIZED VIEW dup_dirs_contraction AS
--WITH cte0 AS (
--    select
--        array_agg(path_id) path_id_list,
--        --array_agg(parent_path_id) parent_path_id_list,
--        level,
--        checksum_list
--    from dup_dirs_expansion
--    group by level, checksum_list
--)
--select * from cte0
--;
--
--DROP MATERIALIZED VIEW IF EXISTS dup_dirs_subtraction CASCADE;
--CREATE MATERIALIZED VIEW dup_dirs_subtraction AS
--WITH cte0 AS (
--    select
--        dde1.path_id, dde2.path_id, ARRAY[(unnest(dde1.checksum_list) - unnest(dde2.checksum_list)) diff
--    from dup_dirs_expansion dde1
--    join dup_dirs_expansion dde2
--    where dde1.level = dde2.level
--    and dde1.path_id != dde2.path_id
--)
--select * from cte0
--;

--DROP MATERIALIZED VIEW IF EXISTS dup_dirs_subtraction CASCADE;
--CREATE MATERIALIZED VIEW dup_dirs_subtraction AS
--SELECT
--    dde1.path_id path_id1,
--    dde2.path_id path_id2,
--    (
--        SELECT array_agg(i) FROM unnest(dde1.checksum_list) AS arr(i)
--        WHERE NOT ARRAY[i] <@ dde2.checksum_list
--    ) AS unique
--FROM dup_dirs_expansion dde1
--JOIN dup_dirs_expansion dde2
--ON dde1.path_id != dde2.path_id
--WHERE dde1.level = dde2.level
--;

DROP MATERIALIZED VIEW IF EXISTS dup_dirs CASCADE;
CREATE MATERIALIZED VIEW dup_dirs AS
    select
        path_id,
        parent_path_id,
        MAX(level) as level,
        SUM(unique_count) unique_count,
        SUM(duplicate_count) duplicate_count,
        SUM(unique_size_in_bytes) unique_size_in_bytes,
        SUM(dup_size_in_bytes) dup_size_in_bytes
    from dir_totals_expansion
    GROUP BY (path_id, parent_path_id)
;

DROP MATERIALIZED VIEW IF EXISTS subdir_counts_for_path CASCADE;
CREATE MATERIALIZED VIEW subdir_counts_for_path AS
    SELECT parent_path_id path_id, COUNT(*) subdir_count FROM path GROUP BY parent_path_id
;

DROP MATERIALIZED VIEW IF EXISTS dup_counts_for_path CASCADE;
CREATE MATERIALIZED VIEW dup_counts_for_path AS
SELECT
    dt.*,
    COALESCE(sc.subdir_count, 0) subdir_count
    FROM dir_totals dt
    LEFT OUTER JOIN subdir_counts_for_path sc USING(path_id)
order by path_id
;

-- WIP: need to verify this
DROP MATERIALIZED VIEW IF EXISTS dup_detail CASCADE;
CREATE MATERIALIZED VIEW dup_detail AS
WITH branch_nodes AS (
    select f.id file_id, l.id location_id, p.id path_id, f.checksum
    FROM path p
    JOIN location l on p.id = l.path_id
    JOIN file f on l.id = f.location_id
    where
    p.id in (select distinct path_id from location)
),
dups AS (
    select checksum, sum(size_in_bytes) dup_disk_usage_agg, count(*)
    FROM file group by checksum
    -- NOTE: include rows for unique files here, as an aid in data verification
    --having count(*) > 1
)
select d.count dups, d.dup_disk_usage_agg, bn.*
FROM branch_nodes bn
JOIN dups d using(checksum)
order by d.dup_disk_usage_agg desc
;

-- WIP: this isn't right, but is it worth more work?
-- WIP: can this be tossed...is it implemented elsewhere, or what?
--DROP MATERIALIZED VIEW IF EXISTS dup_counts_by_subpath CASCADE;
--CREATE MATERIALIZED VIEW dup_counts_by_subpath AS
--CREATE MATERIALIZED VIEW dup_counts_by_subpath AS
--WITH RECURSIVE x AS (
--    SELECT path_id, parent_path_id, unique_count, duplicate_count, dup_disk_usage_agg, subdir_count
--    FROM dup_counts_for_path
--    UNION ALL
--    SELECT parent.id path_id, parent.parent_path_id, child.unique_count, child.duplicate_count, child.dup_disk_usage_agg, child.subdir_count
--    FROM path parent
--    JOIN x child ON parent.id = child.parent_path_id
--)
--SELECT * FROM x
--ORDER BY dup_disk_usage_agg desc
--;
--;

-- WIP - WTF is going on with this?
--DROP MATERIALIZED VIEW IF EXISTS dup_counts_for_file CASCADE;
--CREATE MATERIALIZED VIEW dup_counts_for_file AS
--WITH
--dup_containing_dirs AS (
--    SELECT distinct path_id
--    FROM dup_counts_for_path
--    WHERE duplicate_count > 1
--),
--files_for_path AS (
--    -- NOTE: dup_disk_usage_agg indicates usage by all copies of each path here.
--    SELECT f.id, COUNT(*) dups, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.checksum ORDER BY f.checksum) checksum_list
--    FROM dup_containing_dirs dd
--    JOIN location l ON dd.path_id = l.path_id
--    JOIN file f ON f.location_id = l.id
--    GROUP BY(dd.path_id)
--)
--SELECT * FROM dup_dirs
--WHERE ARRAY_LENGTH(dup_path_ids, 1) > 1
--ORDER BY dup_disk_usage_agg desc
--;

DROP MATERIALIZED VIEW IF EXISTS exact_dup_dirs CASCADE;
CREATE MATERIALIZED VIEW exact_dup_dirs AS
WITH
duplicate_files as (
    SELECT file_ids FROM file_dup_groups
    WHERE ARRAY_LENGTH(file_ids, 1) > 1
),
dup_containing_dirs AS (
    SELECT distinct path_id
    FROM dup_counts_for_path
    WHERE duplicate_count > 1
),
files_for_path AS (
    -- NOTE: dup_disk_usage_agg indicates usage by all copies of each path here.
    SELECT dd.path_id, SUM(f.size_in_bytes) dup_disk_usage_agg, array_agg(f.checksum ORDER BY f.checksum) checksum_list
    FROM dup_containing_dirs dd
    JOIN location l ON dd.path_id = l.path_id
    JOIN file f ON f.location_id = l.id
    GROUP BY(dd.path_id)
),
dup_dirs AS (
    SELECT SUM(dup_disk_usage_agg) dup_disk_usage_agg, array_agg(distinct path_id) dup_path_ids
    FROM files_for_path
    GROUP BY(checksum_list)
)
SELECT * FROM dup_dirs
WHERE ARRAY_LENGTH(dup_path_ids, 1) > 1
ORDER BY dup_disk_usage_agg desc
;

--DROP MATERIALIZED VIEW IF EXISTS super_dirs CASCADE;
--CREATE MATERIALIZED VIEW super_dirs AS
--WITH
--dup_containing_dirs AS (
--    SELECT distinct path_id
--    FROM dup_counts_for_path
--    WHERE duplicate_count > 1
--),
--all_checksums_for_path AS (
--    SELECT dd.path_id, array_agg(f.checksum ORDER BY f.checksum) checksum_list
--    FROM dup_containing_dirs dd
--    JOIN location l ON dd.path_id = l.path_id
--    JOIN file f ON f.location_id = l.id
--    GROUP BY(dd.path_id)
--),
--paths_contained_by_path AS (
--    SELECT p1.path_id path_id, array_agg(distinct p2.path_id) subset_dirs
--    FROM all_checksums_for_path p1, all_checksums_for_path p2
--    WHERE p1.path_id <> p2.path_id
--    -- does p1's file list contain p2's list?
--    AND p1.checksum_list @> p2.checksum_list
--    GROUP BY(p1.path_id)
--)
--SELECT array_prepend(path_id, subset_dirs) AS paths_contained_by_path FROM paths_contained_by_path
--;

-- describe tables, views and sequences
\d

