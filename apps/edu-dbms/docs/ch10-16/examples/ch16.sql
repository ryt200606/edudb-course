-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch16_rows(id INTEGER PRIMARY KEY,v VARCHAR(80));
INSERT INTO ch16_rows VALUES(1,'base');
CHECKPOINT;
SHOW WAL;
SHOW RECOVERY;
SET ARCHIVING ON;
BACKUP DATABASE AS 'chapter16_base';
UPDATE ch16_rows SET v='target_commit' WHERE id=1;
ARCHIVE WAL;
VERIFY;
