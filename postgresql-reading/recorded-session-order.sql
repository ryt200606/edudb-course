
-- 2026-10-31T07:04:20.426892+00:00 session=admin pid=1632
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:20.428964+00:00 session=admin pid=1632
SHOW server_version_num

-- 2026-10-31T07:04:20.429787+00:00 session=admin pid=1632
SHOW data_directory

-- 2026-10-31T07:04:20.435050+00:00 session=admin pid=1632
SELECT name,setting,unit,source FROM pg_settings WHERE name IN ('shared_buffers','max_parallel_workers','max_parallel_workers_per_gather','autovacuum','fsync','synchronous_commit','full_page_writes','checkpoint_timeout','port','listen_addresses') ORDER BY name

-- 2026-10-31T07:04:21.245205+00:00 session=index_plain pid=1741
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:21.257858+00:00 session=index_plain pid=1741
CREATE TABLE probe(id integer PRIMARY KEY,k integer NOT NULL,payload text NOT NULL);

-- 2026-10-31T07:04:22.463298+00:00 session=index_plain pid=1741
INSERT INTO probe SELECT g,g%1000,repeat('x',64) FROM generate_series(1,120000) g;

-- 2026-10-31T07:04:22.736768+00:00 session=index_plain pid=1741
CREATE INDEX probe_k_idx ON probe(k); ANALYZE probe;

-- 2026-10-31T07:04:22.738669+00:00 session=index_plain pid=1741
SELECT 'probe'::regclass::oid,'probe_pkey'::regclass::oid,'probe_k_idx'::regclass::oid,pg_relation_filenode('probe'),pg_relation_size('probe'),pg_relation_size('probe_pkey')

-- 2026-10-31T07:04:22.740606+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020

-- 2026-10-31T07:04:22.773199+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000

-- 2026-10-31T07:04:22.775053+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE k=42

-- 2026-10-31T07:04:22.776363+00:00 session=index_plain pid=1741
SET enable_indexscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=on;

-- 2026-10-31T07:04:22.797349+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020

-- 2026-10-31T07:04:22.818082+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020 ORDER BY id

-- 2026-10-31T07:04:22.819442+00:00 session=index_plain pid=1741
SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=off;

-- 2026-10-31T07:04:22.821004+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020

-- 2026-10-31T07:04:22.822399+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020 ORDER BY id

-- 2026-10-31T07:04:22.823148+00:00 session=index_plain pid=1741
SET enable_indexscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=on;

-- 2026-10-31T07:04:22.851911+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000

-- 2026-10-31T07:04:24.873317+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000 ORDER BY id

-- 2026-10-31T07:04:25.264870+00:00 session=index_plain pid=1741
SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=off;

-- 2026-10-31T07:04:25.320056+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000

-- 2026-10-31T07:04:26.691756+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000 ORDER BY id

-- 2026-10-31T07:04:27.054007+00:00 session=index_plain pid=1741
SET enable_indexscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=on;

-- 2026-10-31T07:04:27.077291+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE k=42

-- 2026-10-31T07:04:27.103101+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE k=42 ORDER BY id

-- 2026-10-31T07:04:27.105238+00:00 session=index_plain pid=1741
SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=off;

-- 2026-10-31T07:04:27.106883+00:00 session=index_plain pid=1741
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id,k,payload FROM probe WHERE k=42

-- 2026-10-31T07:04:27.109856+00:00 session=index_plain pid=1741
SELECT id,k,payload FROM probe WHERE k=42 ORDER BY id

-- 2026-10-31T07:04:27.110831+00:00 session=index_plain pid=1741
RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan; RESET enable_seqscan;

-- 2026-10-31T07:04:27.120307+00:00 session=index_gdb pid=2168
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:27.121181+00:00 session=index_gdb pid=2168
SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=off; SET enable_seqscan=off;

-- 2026-10-31T07:04:29.084944+00:00 session=index_gdb pid=2168
SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020 ORDER BY id

-- 2026-10-31T07:04:29.513185+00:00 session=admin pid=1632
CREATE TABLE mv_probe(id integer PRIMARY KEY,v integer NOT NULL); INSERT INTO mv_probe VALUES(1,10),(2,20);

-- 2026-10-31T07:04:29.514440+00:00 session=admin pid=1632
SELECT 'mv_probe'::regclass::oid

-- 2026-10-31T07:04:29.520236+00:00 session=rc_a pid=2444
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:29.525416+00:00 session=rc_b pid=2445
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:29.526351+00:00 session=rc_a pid=2444
BEGIN ISOLATION LEVEL READ COMMITTED;

-- 2026-10-31T07:04:29.528434+00:00 session=rc_a pid=2444
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:29.530825+00:00 session=rc_b pid=2445
UPDATE mv_probe SET v=11 WHERE id=1;

-- 2026-10-31T07:04:29.531816+00:00 session=rc_a pid=2444
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:29.532336+00:00 session=rc_a pid=2444
COMMIT;

-- 2026-10-31T07:04:29.535897+00:00 session=rr_a pid=2446
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:29.540857+00:00 session=rr_b pid=2447
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:29.541729+00:00 session=rr_a pid=2446
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- 2026-10-31T07:04:29.543940+00:00 session=rr_a pid=2446
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:29.547003+00:00 session=rr_b pid=2447
UPDATE mv_probe SET v=12 WHERE id=1;

-- 2026-10-31T07:04:29.548048+00:00 session=rr_a pid=2446
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:29.549763+00:00 session=admin pid=1632
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:31.137046+00:00 session=rr_a pid=2446
SELECT id,v,xmin::text,xmax::text,ctid::text,txid_current_snapshot()::text FROM mv_probe WHERE id=1

-- 2026-10-31T07:04:31.138289+00:00 session=rr_a pid=2446
ROLLBACK;

-- 2026-10-31T07:04:31.160382+00:00 session=wait_holder pid=2563
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:31.165426+00:00 session=wait_waiter pid=2564
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:31.168602+00:00 session=wait_holder pid=2563
BEGIN; UPDATE mv_probe SET v=v+1 WHERE id=1;

-- 2026-10-31T07:04:32.071237+00:00 session=admin pid=1632
SELECT pid,application_name,state,wait_event_type,wait_event,pg_blocking_pids(pid)::text FROM pg_stat_activity WHERE pid IN (2563,2564) ORDER BY pid

-- 2026-10-31T07:04:32.073031+00:00 session=admin pid=1632
SELECT pid,locktype,mode,granted,relation::text,page,tuple,transactionid::text,virtualxid FROM pg_locks WHERE pid IN (2563,2564) ORDER BY pid,locktype,mode,granted

-- 2026-10-31T07:04:32.075501+00:00 session=wait_waiter pid=2564
UPDATE mv_probe SET v=v+10 WHERE id=1;

-- 2026-10-31T07:04:32.076323+00:00 session=wait_holder pid=2563
COMMIT;

-- 2026-10-31T07:04:32.111890+00:00 session=admin pid=1632
UPDATE mv_probe SET v=id*100;

-- 2026-10-31T07:04:32.117232+00:00 session=deadlock_a pid=2681
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:32.122797+00:00 session=deadlock_b pid=2682
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:32.123520+00:00 session=deadlock_a pid=2681
SET deadlock_timeout='2s'; SET lock_timeout='12s'; BEGIN;

-- 2026-10-31T07:04:32.124214+00:00 session=deadlock_b pid=2682
SET deadlock_timeout='2s'; SET lock_timeout='12s'; BEGIN;

-- 2026-10-31T07:04:32.126714+00:00 session=deadlock_a pid=2681
UPDATE mv_probe SET v=v+1 WHERE id=1;

-- 2026-10-31T07:04:32.129111+00:00 session=deadlock_b pid=2682
UPDATE mv_probe SET v=v+10 WHERE id=2;

-- 2026-10-31T07:04:32.131913+00:00 session=admin pid=1632
SELECT pid,application_name,state,wait_event_type,wait_event,pg_blocking_pids(pid)::text FROM pg_stat_activity WHERE pid IN (2681,2682) ORDER BY pid

-- 2026-10-31T07:04:32.133442+00:00 session=admin pid=1632
SELECT pid,locktype,mode,granted,relation::text,page,tuple,transactionid::text,virtualxid FROM pg_locks WHERE pid IN (2681,2682) ORDER BY pid,locktype,mode,granted

-- 2026-10-31T07:04:32.136214+00:00 session=admin pid=1632
SELECT pid,application_name,state,wait_event_type,wait_event,pg_blocking_pids(pid)::text FROM pg_stat_activity WHERE pid IN (2681,2682) ORDER BY pid

-- 2026-10-31T07:04:32.137392+00:00 session=admin pid=1632
SELECT pid,locktype,mode,granted,relation::text,page,tuple,transactionid::text,virtualxid FROM pg_locks WHERE pid IN (2681,2682) ORDER BY pid,locktype,mode,granted

-- 2026-10-31T07:04:34.132740+00:00 session=deadlock_a pid=2681
UPDATE mv_probe SET v=v+1 WHERE id=2;

-- 2026-10-31T07:04:34.133350+00:00 session=deadlock_b pid=2682
UPDATE mv_probe SET v=v+10 WHERE id=1;

-- 2026-10-31T07:04:34.135311+00:00 session=deadlock_a pid=2681
ROLLBACK;

-- 2026-10-31T07:04:34.136357+00:00 session=deadlock_b pid=2682
COMMIT;

-- 2026-10-31T07:04:34.138356+00:00 session=admin pid=1632
SELECT id,v FROM mv_probe ORDER BY id

-- 2026-10-31T07:04:35.037538+00:00 session=admin pid=1632
CREATE TABLE wal_probe(id integer PRIMARY KEY,value text NOT NULL); INSERT INTO wal_probe VALUES(0,'baseline'); CHECKPOINT;

-- 2026-10-31T07:04:35.040014+00:00 session=admin pid=1632
SELECT 'wal_probe'::regclass::oid,pg_relation_filenode('wal_probe'),(SELECT oid FROM pg_database WHERE datname=current_database())

-- 2026-10-31T07:04:35.041262+00:00 session=admin pid=1632
SELECT pg_current_wal_insert_lsn()::text

-- 2026-10-31T07:04:35.048753+00:00 session=wal_committed_a pid=2951
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:35.051141+00:00 session=wal_committed_a pid=2951
BEGIN; INSERT INTO wal_probe VALUES(1001,'committed-a');

-- 2026-10-31T07:04:35.052586+00:00 session=wal_committed_a pid=2951
SELECT txid_current()::text

-- 2026-10-31T07:04:36.091245+00:00 session=wal_committed_a pid=2951
COMMIT;

-- 2026-10-31T07:04:36.108783+00:00 session=admin pid=1632
SELECT pg_current_wal_flush_lsn()::text

-- 2026-10-31T07:04:36.115403+00:00 session=wal_uncommitted_b pid=3020
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:36.117232+00:00 session=wal_uncommitted_b pid=3020
BEGIN; INSERT INTO wal_probe VALUES(1002,'uncommitted-b');

-- 2026-10-31T07:04:36.118583+00:00 session=wal_uncommitted_b pid=3020
SELECT txid_current()::text

-- 2026-10-31T07:04:36.119188+00:00 session=admin pid=1632
SELECT pg_current_wal_insert_lsn()::text

-- 2026-10-31T07:04:36.120948+00:00 session=wal_uncommitted_b pid=3020
SELECT id,value FROM wal_probe ORDER BY id

-- 2026-10-31T07:04:36.122473+00:00 session=admin pid=1632
SELECT id,value FROM wal_probe ORDER BY id

-- 2026-10-31T07:04:36.127270+00:00 session=admin pid=1632
SELECT pg_switch_wal()::text

-- 2026-10-31T07:04:36.129054+00:00 session=admin pid=1632
SELECT pg_current_wal_flush_lsn()::text

-- 2026-10-31T07:04:36.485102+00:00 session=post_crash_verify pid=3081
SET statement_timeout='20s'; SET lock_timeout='12s'; SET idle_in_transaction_session_timeout='120s';

-- 2026-10-31T07:04:36.488107+00:00 session=post_crash_verify pid=3081
SELECT id,value FROM wal_probe ORDER BY id

-- 2026-10-31T07:04:36.489471+00:00 session=post_crash_verify pid=3081
SELECT pg_is_in_recovery()::text

-- 2026-10-31T07:04:36.490871+00:00 session=post_crash_verify pid=3081
SELECT id,value FROM wal_probe ORDER BY id

-- 2026-10-31T07:04:36.491519+00:00 session=post_crash_verify pid=3081
SHOW fsync

-- 2026-10-31T07:04:36.492011+00:00 session=post_crash_verify pid=3081
SHOW synchronous_commit

-- 2026-10-31T07:04:36.492554+00:00 session=post_crash_verify pid=3081
SHOW full_page_writes
