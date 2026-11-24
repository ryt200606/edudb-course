-- Use a fresh database and execute with edudb run --url <service> --file course-integrity.sql.
-- The expected final rows include a NULL grade and an exact 64-bit student number.
CREATE TABLE students(id BIGINT PRIMARY KEY, name VARCHAR(24) NOT NULL);
CREATE TABLE courses(term INTEGER, code VARCHAR(8), title VARCHAR(24) UNIQUE, PRIMARY KEY(term,code));
CREATE TABLE enrolment(
    sid BIGINT, term INTEGER, code VARCHAR(8), grade INTEGER,
    PRIMARY KEY(sid,term,code),
    CONSTRAINT valid_grade CHECK(grade>=0 AND grade<=100),
    CONSTRAINT enrol_student FOREIGN KEY(sid) REFERENCES students(id) ON DELETE CASCADE,
    CONSTRAINT enrol_course FOREIGN KEY(term,code) REFERENCES courses(term,code) ON DELETE RESTRICT
);
INSERT INTO students VALUES (1,'陈同学'),(9007199254740993,'王同学');
INSERT INTO courses VALUES (1,'db','数据库系统');
INSERT INTO enrolment VALUES (1,1,'db',90),(9007199254740993,1,'db',NULL);
DESCRIBE enrolment;
BEGIN;
DELETE FROM students WHERE id=1;
SELECT COUNT(*) AS after_cascade FROM enrolment;
ROLLBACK;
SELECT e.sid,s.name,c.title,e.grade
FROM enrolment e JOIN students s ON e.sid=s.id JOIN courses c ON e.term=c.term AND e.code=c.code
ORDER BY e.sid;
