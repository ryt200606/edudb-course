\set ON_ERROR_STOP 1
\pset pager off
-- All user objects below live in this session's temporary schema.
CREATE TEMP TABLE corr(id int PRIMARY KEY, a int, b int);
CREATE TEMP TABLE uniform_pairs (LIKE corr INCLUDING ALL);
INSERT INTO corr SELECT i,i%10,i%10 FROM generate_series(0,999) i;
INSERT INTO uniform_pairs SELECT i,i%10,(i/10)%10 FROM generate_series(0,999) i;
ANALYZE corr;
ANALYZE uniform_pairs;
SELECT tablename,attname,n_distinct,most_common_freqs
FROM pg_stats WHERE schemaname=pg_my_temp_schema()::regnamespace::text
AND tablename IN ('corr','uniform_pairs') AND attname IN ('a','b') ORDER BY 1,2;
\echo 'Ordinary column statistics, compare estimated rows and actual rows'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM corr WHERE a=2 AND b=2;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM uniform_pairs WHERE a=2 AND b=2;
CREATE STATISTICS pg_temp.corr_mcv (mcv) ON a,b FROM corr;
CREATE STATISTICS pg_temp.uniform_mcv (mcv) ON a,b FROM uniform_pairs;
ANALYZE corr;
ANALYZE uniform_pairs;
\echo 'Joint MCV, compare matching and contradictory values'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM corr WHERE a=2 AND b=2;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM corr WHERE a=2 AND b=3;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM uniform_pairs WHERE a=2 AND b=2;
DROP STATISTICS pg_temp.corr_mcv;
CREATE STATISTICS pg_temp.corr_fd (dependencies) ON a,b FROM corr;
ANALYZE corr;
\echo 'Functional dependency, same marginal values, contradictory pair'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM corr WHERE a=2 AND b=2;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM corr WHERE a=2 AND b=3;
CREATE TEMP TABLE cover_rows(id int NOT NULL,label int,payload text) WITH (fillfactor=60);
INSERT INTO cover_rows SELECT i,i%20,repeat('x',160) FROM generate_series(1,3000) i;
CREATE UNIQUE INDEX cover_idx ON cover_rows(id) INCLUDE(label);
VACUUM (ANALYZE) cover_rows;
\echo 'Watch Heap Fetches on id=250 and its untouched control id=2250'
EXPLAIN (ANALYZE, BUFFERS) SELECT id,label FROM cover_rows WHERE id=250;
UPDATE cover_rows SET payload=repeat('y',160) WHERE id=250;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,label FROM cover_rows WHERE id=250;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,label FROM cover_rows WHERE id=2250;
VACUUM (DISABLE_PAGE_SKIPPING) cover_rows;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,label FROM cover_rows WHERE id=250;
