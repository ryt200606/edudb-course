-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch13_rows(id INTEGER PRIMARY KEY,g VARCHAR(8),x REAL);
INSERT INTO ch13_rows VALUES(1,'a',1.0),(2,'a',2.0),(3,'b',3.0);
ANALYZE ch13_rows;
EXPLAIN ANALYZE SELECT id,x FROM ch13_rows ORDER BY x DESC LIMIT 2;
EXPLAIN ANALYZE SELECT a.id,b.x FROM ch13_rows a JOIN ch13_rows b ON a.id=b.id;
CREATE MATERIALIZED VIEW ch13_mv WITH(refresh='immediate',maintenance='incremental') AS SELECT g,COUNT(*) AS n,SUM(x) AS total,AVG(x) AS mean FROM ch13_rows GROUP BY g;
UPDATE ch13_rows SET x=4.0 WHERE id=1;
SELECT * FROM ch13_mv ORDER BY g;
SHOW MATERIALIZED VIEWS;
