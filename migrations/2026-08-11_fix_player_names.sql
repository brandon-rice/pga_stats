-- ============================================================================
-- Merge split player identities in pga_stats.leaderboard_data
-- Generated 2026-08-11
-- ============================================================================
--
-- WHY
--   leaderboard_data stores some players under two spellings (a nickname and a
--   formal name, or an accented and an unaccented form). sg_data does not — it
--   uses one canonical spelling per player. Because StrokesGainedAnalysisv2.py
--   joins the two tables on `a.player = b.player`, every leaderboard row filed
--   under the "wrong" spelling silently fails to find its strokes-gained row,
--   and this line then invents a value for it:
--
--       case when b.avg is null then -2 else b.avg end
--
--   So a spelling mismatch does not merely lose data, it fabricates a -2
--   strokes-gained result and feeds it into every SG_last_N rolling average.
--   Sam Stevens currently has 63 events scored this way.
--
--   Splitting also means groupby('player') computes rolling averages, average
--   finish, and top-N counts over a partial career.
--
-- SCOPE
--   161 rows in leaderboard_data and 29 rows in sg_data, across 7 players.
--   Repairs 63 fabricated -2 results. The amateur "(a)" marker is deliberately
--   NOT touched. Verified by dry run against brdb on 2026-08-11: row totals
--   unchanged, no duplicate player/event rows, no player left worse off.
--
-- CANONICAL SPELLING
--   Taken from sg_data, not from whichever spelling is more common in
--   leaderboard_data. Frequency would pick the wrong side for Niklas Nørgaard
--   (18 unaccented leaderboard rows, but sg_data uses the accented form) and
--   would leave the join broken.
--
-- SAFETY
--   Runs in one transaction and snapshots both tables first. Every rename is
--   guarded: if merging two spellings would ever put the same player in the
--   same event twice, the whole migration aborts. Verified as of 2026-08-11
--   that all 8 renames are collision-free.
--
-- HOW TO RUN
--   psql "$DATABASE_URL" -f migrations/2026-08-11_fix_player_names.sql
--
--   For a dry run, change the final COMMIT to ROLLBACK. The guards, the
--   UPDATEs, and the row counts all still execute and report.
--
-- ROLLBACK AFTER COMMIT
--   UPDATE pga_stats.leaderboard_data l SET player = b.player
--     FROM pga_stats.leaderboard_data_backup_20260811 b WHERE l.ctid = b.ctid;
--   -- or simply restore from the backup tables created below.
-- ============================================================================

BEGIN;

-- ── 1. Snapshot both tables ────────────────────────────────────────────────
DROP TABLE IF EXISTS pga_stats.leaderboard_data_backup_20260811;
CREATE TABLE pga_stats.leaderboard_data_backup_20260811 AS
    SELECT * FROM pga_stats.leaderboard_data;

DROP TABLE IF EXISTS pga_stats.sg_data_backup_20260811;
CREATE TABLE pga_stats.sg_data_backup_20260811 AS
    SELECT * FROM pga_stats.sg_data;


-- ── 2. The mapping, defined once ───────────────────────────────────────────
CREATE TEMP TABLE player_name_fix (
    variant   text PRIMARY KEY,
    canonical text NOT NULL,
    note      text
) ON COMMIT DROP;

INSERT INTO player_name_fix (variant, canonical, note) VALUES
    -- Nickname / formal-name pairs. sg_data uses the canonical side.
    ('Samuel Stevens',        'Sam Stevens',            '63 rows; recovers 37 SG rows'),
    ('Matthew NeSmith',       'Matt NeSmith',           '72 rows; recovers 17 SG rows'),
    ('Matthew Fitzpatrick',   'Matt Fitzpatrick',       '3 rows; recovers 3 SG rows'),
    ('John Keefer',           'Johnny Keefer',          '1 row; recovers 1 SG row'),
    -- Accent / case variants.
    ('Nicolai Hojgaard',      'Nicolai Højgaard',       '1 row; recovers 5 SG rows via the sg_data rename'),
    ('Niklas Norgaard',       'Niklas Nørgaard',        '18 rows; SG-neutral, but merges a split career'),
    ('Santiago de la Fuente', 'Santiago De la Fuente',  '3 rows; SG-neutral, sg_data is itself split here');

-- Pairs deliberately left out — 1-3 rows each, no strokes-gained data on
-- either side, so no SG impact and no evidence for which spelling is right.
-- Uncomment individually once confirmed by hand:
--     ('Wesley Heffernan', 'Wes Heffernan',    '1 row'),
--     ('Jack Buchanan',    'Jackson Buchanan', '1 row'),
--     ('Ockert Strydom',   'Ockie Strydom',    '3 rows'),


-- ── 3. Abort if any rename would double-book a player in one event ─────────
DO $$
DECLARE
    r      record;
    dupes  integer;
BEGIN
    FOR r IN SELECT variant, canonical FROM player_name_fix LOOP
        SELECT count(*) INTO dupes FROM (
            SELECT year, week_of_season
            FROM pga_stats.leaderboard_data
            WHERE player IN (r.variant, r.canonical)
            GROUP BY year, week_of_season
            HAVING count(*) > 1
        ) t;

        IF dupes > 0 THEN
            RAISE EXCEPTION
                'Aborting: merging % into % would create % duplicate event row(s)',
                r.variant, r.canonical, dupes;
        END IF;
    END LOOP;

    RAISE NOTICE 'Collision check passed for % rename(s)',
        (SELECT count(*) FROM player_name_fix);
END $$;


-- ── 4. Apply to leaderboard_data ───────────────────────────────────────────
UPDATE pga_stats.leaderboard_data AS l
SET    player = f.canonical
FROM   player_name_fix AS f
WHERE  l.player = f.variant;


-- ── 5. Apply the SAME mapping to sg_data ───────────────────────────────────
-- This is not optional. sg_data carries both spellings for Nicolai Højgaard,
-- Niklas Nørgaard, Santiago de la Fuente and Matt NeSmith, so renaming only
-- leaderboard_data would break rows that currently DO match. A dry run of an
-- earlier version of this script, which touched leaderboard_data alone, moved
-- Niklas Nørgaard from 12 missing SG rows to 20. Both tables move together.
DO $$
DECLARE
    r      record;
    dupes  integer;
BEGIN
    FOR r IN SELECT variant, canonical FROM player_name_fix LOOP
        SELECT count(*) INTO dupes FROM (
            SELECT year, week_of_season
            FROM pga_stats.sg_data
            WHERE player IN (r.variant, r.canonical)
            GROUP BY year, week_of_season
            HAVING count(*) > 1
        ) t;

        IF dupes > 0 THEN
            RAISE EXCEPTION
                'Aborting: merging % into % on sg_data would create % duplicate row(s)',
                r.variant, r.canonical, dupes;
        END IF;
    END LOOP;

    RAISE NOTICE 'sg_data collision check passed';
END $$;

UPDATE pga_stats.sg_data AS s
SET    player = f.canonical
FROM   player_name_fix AS f
WHERE  s.player = f.variant;


-- ── 6. Report what changed ─────────────────────────────────────────────────
DO $$
DECLARE
    lb_changed integer;
    sg_changed integer;
BEGIN
    SELECT count(*) INTO lb_changed
    FROM pga_stats.leaderboard_data_backup_20260811 b
    JOIN player_name_fix f ON b.player = f.variant;

    SELECT count(*) INTO sg_changed
    FROM pga_stats.sg_data_backup_20260811 b
    JOIN player_name_fix f ON b.player = f.variant;

    RAISE NOTICE 'leaderboard_data rows renamed: %', lb_changed;
    RAISE NOTICE 'sg_data rows renamed:          %', sg_changed;
END $$;

COMMIT;


-- ============================================================================
-- VERIFICATION — run after the migration. The backup tables hold the "before"
-- state, so each query compares live against backup.
-- ============================================================================

-- 6a. No variant spelling should survive in leaderboard_data.
SELECT player, count(*) AS rows
FROM   pga_stats.leaderboard_data
WHERE  player IN ('Samuel Stevens', 'Matthew NeSmith', 'Matthew Fitzpatrick',
                  'John Keefer', 'Nicolai Hojgaard', 'Niklas Norgaard',
                  'Santiago de la Fuente')
GROUP  BY player;
-- expected: 0 rows


-- 6b. Row totals must be unchanged — this renames, it never deletes.
SELECT (SELECT count(*) FROM pga_stats.leaderboard_data)            AS lb_now,
       (SELECT count(*) FROM pga_stats.leaderboard_data_backup_20260811) AS lb_before,
       (SELECT count(*) FROM pga_stats.sg_data)                     AS sg_now,
       (SELECT count(*) FROM pga_stats.sg_data_backup_20260811)     AS sg_before;
-- expected: lb_now = lb_before, sg_now = sg_before


-- 6c. The point of the exercise: strokes-gained coverage, before vs after.
--     "missing_sg" rows are the ones scored as a fabricated -2.
WITH before AS (
    SELECT b.player,
           count(*) FILTER (WHERE s.player IS NULL) AS missing_sg,
           count(*)                                  AS events
    FROM   pga_stats.leaderboard_data_backup_20260811 b
    LEFT   JOIN pga_stats.sg_data_backup_20260811 s
           ON  b.player = s.player
           AND b.year = s.year
           AND b.week_of_season = s.week_of_season
    WHERE  b.player IN ('Samuel Stevens', 'Sam Stevens', 'Matthew NeSmith',
                        'Matt NeSmith', 'Matthew Fitzpatrick', 'Matt Fitzpatrick',
                        'John Keefer', 'Johnny Keefer', 'Niklas Norgaard',
                        'Niklas Nørgaard', 'Nicolai Hojgaard', 'Nicolai Højgaard')
    GROUP  BY b.player
), after AS (
    SELECT l.player,
           count(*) FILTER (WHERE s.player IS NULL) AS missing_sg,
           count(*)                                  AS events
    FROM   pga_stats.leaderboard_data l
    LEFT   JOIN pga_stats.sg_data s
           ON  l.player = s.player
           AND l.year = s.year
           AND l.week_of_season = s.week_of_season
    WHERE  l.player IN ('Sam Stevens', 'Matt NeSmith', 'Matt Fitzpatrick',
                        'Johnny Keefer', 'Niklas Nørgaard', 'Nicolai Højgaard')
    GROUP  BY l.player
)
SELECT COALESCE(a.player, b.player)  AS player,
       b.events      AS events_before,
       b.missing_sg  AS missing_sg_before,
       a.events      AS events_after,
       a.missing_sg  AS missing_sg_after
FROM   after a
FULL   OUTER JOIN before b ON a.player = b.player
ORDER  BY 1;
-- expected: for each merged player, events_after = sum of both spellings'
-- events_before, and missing_sg_after well below the sum of missing_sg_before


-- 6d. Nobody should now appear twice in the same event.
SELECT player, year, week_of_season, count(*)
FROM   pga_stats.leaderboard_data
GROUP  BY player, year, week_of_season
HAVING count(*) > 1
ORDER  BY 4 DESC;
-- expected: 0 rows


-- ============================================================================
-- AFTERWARDS
--   This repairs existing rows only. leaderboard_upload.py does per-tournament
--   INSERTs (see leaderboard_upload.py:174) and never rebuilds the table, so
--   the next upload can reintroduce a variant spelling. Apply the same mapping
--   at upload time, or re-run this migration after each load.
--
--   Once satisfied, drop the snapshots:
--     DROP TABLE pga_stats.leaderboard_data_backup_20260811;
--     DROP TABLE pga_stats.sg_data_backup_20260811;
-- ============================================================================
