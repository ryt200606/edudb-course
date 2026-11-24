-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch10_rows(id INTEGER PRIMARY KEY,v REAL,note VARCHAR(20)) RECORD FORMAT VARIABLE;
INSERT INTO ch10_rows VALUES(1,1.5,'中文'),(2,NULL,'变长记录');
DESCRIBE ch10_rows;
ALTER TABLE ch10_rows RECORD FORMAT FIXED;
DESCRIBE ch10_rows;
CREATE TABLE ch10_ordered(id INTEGER PRIMARY KEY,k VARCHAR(16)) ORGANIZATION ORDERED(k);
INSERT INTO ch10_ordered VALUES(1,'b'),(2,'a');
SELECT * FROM ch10_ordered ORDER BY k;
ANALYZE ch10_ordered;
SHOW BUFFER;
SHOW MEDIA;
VERIFY;
