-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch12_a(id INTEGER PRIMARY KEY,k INTEGER);
CREATE TABLE ch12_b(id INTEGER PRIMARY KEY,k INTEGER);
INSERT INTO ch12_a VALUES(1,1),(2,2),(3,NULL);
INSERT INTO ch12_b VALUES(11,1),(12,1),(13,3);
SELECT a.id,b.id FROM ch12_a a LEFT JOIN ch12_b b ON a.k=b.k ORDER BY a.id,b.id;
SELECT k FROM ch12_a UNION ALL SELECT k FROM ch12_b ORDER BY k;
SELECT k,COUNT(*) AS n FROM ch12_a GROUP BY k ORDER BY k;
EXPLAIN ANALYZE SELECT a.id,b.id FROM ch12_a a JOIN ch12_b b ON a.k=b.k USE JOIN(HASH);
