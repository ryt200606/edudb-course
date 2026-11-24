-- Static PAGE ARIES example; NOT_RUN by this documentation audit.
-- Use a fresh ARIES_PAGE database. Backup/archive/physical standby require REDO_PRIVATE.
CREATE TABLE ch16_page(id INTEGER PRIMARY KEY,v VARCHAR(80));
INSERT INTO ch16_page VALUES(1,'base');
BEGIN;
SAVEPOINT before_update;
UPDATE ch16_page SET v='rolled_back' WHERE id=1;
ROLLBACK TO SAVEPOINT before_update;
RELEASE SAVEPOINT before_update;
COMMIT;
SELECT * FROM ch16_page ORDER BY id;
CHECKPOINT;
SHOW WAL;
SHOW WAL RECORDS LIMIT 32;
SHOW RECOVERY;
VERIFY;
