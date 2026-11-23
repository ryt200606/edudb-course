-- EduDB chapter10-16 development candidate, use a NEW isolated database.
-- Example creation: edudb serve --data NEW_DIR --data-layout RAID6 --media-members 6
-- Existing accepted v3 video and published executable do not include this batch.
SHOW MEDIA;
CREATE TABLE chapter10_demo(id INTEGER PRIMARY KEY, description VARCHAR(200), score INTEGER);
INSERT INTO chapter10_demo VALUES(1,'中文页与缓冲',91),(2,'索引及恢复',NULL),(3,'事务中的修改',87);
CREATE INDEX chapter10_score ON chapter10_demo(score);
SET BUFFER POLICY CLOCK;
SET BUFFER READ_AHEAD 2;
SELECT id,description FROM chapter10_demo ORDER BY id;
SHOW BUFFER;
EXPLAIN ANALYZE SELECT id FROM chapter10_demo WHERE score >= 90;
SET BUFFER POLICY TOSS_IMMEDIATE;
SELECT COUNT(*) FROM chapter10_demo;
SHOW BUFFER;
SET BUFFER POLICY LRU;
SET BUFFER READ_AHEAD 0;
ANALYZE chapter10_demo;
SHOW TRANSACTIONS;
SHOW LOCKS;
SHOW WAL;
CHECKPOINT;
-- SCRUB reads durable member files; transaction-private images are excluded.
SCRUB MEDIA;
VERIFY;
