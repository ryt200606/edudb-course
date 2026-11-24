-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch14_rows(id INTEGER PRIMARY KEY,v INTEGER);
INSERT INTO ch14_rows VALUES(1,10);
BEGIN;
SAVEPOINT before_update;
UPDATE ch14_rows SET v=99 WHERE id=1;
ROLLBACK TO SAVEPOINT before_update;
SELECT v FROM ch14_rows WHERE id=1;
RELEASE SAVEPOINT before_update;
COMMIT;
BEGIN PROTOCOL SNAPSHOT ISOLATION LEVEL READ COMMITTED;
SELECT * FROM ch14_rows;
COMMIT;
SHOW TRANSACTIONS;
