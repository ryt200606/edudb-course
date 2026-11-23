-- Run with: edudb run --file demo/01_core.sql
-- Every successful run starts from a fresh dedicated data directory.
CREATE TABLE students (id INTEGER PRIMARY KEY, name VARCHAR(20) NOT NULL, score REAL, passed BOOLEAN DEFAULT FALSE);
INSERT INTO students(id,name,score) VALUES (1,'张三',86.5),(2,'李四',92),(3,'王五',NULL);
SELECT * FROM students ORDER BY id;
UPDATE students SET passed=TRUE,score=score+1 WHERE score>=90;
ALTER TABLE students ADD COLUMN age INTEGER DEFAULT 20;
ALTER TABLE students RENAME COLUMN age TO years;
SELECT id,name,score,years FROM students WHERE score IS NOT NULL ORDER BY score DESC LIMIT 2;
ALTER TABLE students DROP COLUMN years;
CREATE TABLE electives (student_id INTEGER, course VARCHAR(20));
INSERT INTO electives VALUES(1,'数据库系统原理'),(2,'操作系统');
SELECT s.name,e.course FROM students s INNER JOIN electives e ON s.id=e.student_id ORDER BY s.id;
SELECT COUNT(*),COUNT(score),AVG(score),MIN(score),MAX(score) FROM students;
EXPLAIN SELECT * FROM students WHERE id>=2 ORDER BY id;
SELECT * FROM students WHERE id>=2 ORDER BY id;
BEGIN;
UPDATE students SET score=0 WHERE id=1;
SELECT * FROM students WHERE id=1;
ROLLBACK;
SELECT * FROM students WHERE id=1;
DELETE FROM students WHERE score IS NULL;
ALTER TABLE electives RENAME TO selections;
SHOW TABLES;
DESCRIBE students;
DROP TABLE selections;
CHECKPOINT;
