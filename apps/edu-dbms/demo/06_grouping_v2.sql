CREATE TABLE group_demo(id INTEGER PRIMARY KEY,department VARCHAR(20),amount INTEGER);
INSERT INTO group_demo VALUES(1,'计算机',80),(2,'计算机',90),(3,'通信',NULL),(4,NULL,60);
SELECT department,COUNT(*) AS rows_in_group,SUM(amount) AS total,AVG(amount) AS mean FROM group_demo GROUP BY department ORDER BY total DESC;
SELECT department,COUNT(*) AS n FROM group_demo GROUP BY department HAVING COUNT(*)>=2 ORDER BY n DESC;
EXPLAIN ANALYZE SELECT id FROM group_demo WHERE id>=2 AND id<4 LIMIT 1;
UPDATE group_demo SET amount=amount+1 WHERE id=1;
SELECT * FROM group_demo WHERE id=1;
