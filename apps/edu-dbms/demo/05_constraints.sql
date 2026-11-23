-- Expected-error demonstration. Execute these statements one by one in the SQL shell or web UI.
INSERT INTO students(id,name) VALUES(1,'重复主键');
INSERT INTO students(id,name) VALUES(9,NULL);
ALTER TABLE students DROP COLUMN id;
SELECT missing_column FROM students;
