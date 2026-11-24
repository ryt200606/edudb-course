-- 在新建的演示数据库执行。两张表共用相同的单列取值频数。
CREATE TABLE class_pair(major INTEGER,faculty INTEGER);
CREATE TABLE class_mix(major INTEGER,faculty INTEGER);
INSERT INTO class_pair VALUES (1,1),(1,1),(1,1),(1,1),(2,2),(2,2),(2,2),(2,2);
INSERT INTO class_mix VALUES (1,1),(1,1),(1,2),(1,2),(2,1),(2,1),(2,2),(2,2);
CREATE INDEX pair_major ON class_pair(major);
CREATE INDEX mix_major ON class_mix(major);
ANALYZE class_pair;
ANALYZE class_mix;
-- 此时两张表对 major=2 AND faculty=2 均估 2 行。
EXPLAIN ANALYZE SELECT major,faculty FROM class_pair WHERE major=2 AND faculty=2;
EXPLAIN ANALYZE SELECT major,faculty FROM class_mix WHERE major=2 AND faculty=2;
CREATE STATISTICS pair_mcv (mcv) ON major,faculty FROM class_pair;
CREATE STATISTICS mix_mcv (mcv) ON major,faculty FROM class_mix;
ANALYZE class_pair;
ANALYZE class_mix;
-- 两表预计输出分别为 4、2。强制走 major 单列索引时，两者均需访问 4 个条目。
EXPLAIN ANALYZE SELECT major,faculty FROM class_pair USE INDEX(pair_major) WHERE major=2 AND faculty=2;
EXPLAIN ANALYZE SELECT major,faculty FROM class_mix USE INDEX(mix_major) WHERE major=2 AND faculty=2;
SELECT COUNT(*) FROM class_pair WHERE major=2 AND faculty=2;
SELECT COUNT(*) FROM class_mix WHERE major=2 AND faculty=2;
-- 同一事务内使统计过期，再验证回滚后恢复到原分布。
BEGIN;
INSERT INTO class_pair VALUES (1,2);
SHOW STATISTICS FROM class_pair;
ROLLBACK;
SHOW STATISTICS FROM class_pair;
