-- Static example derived from the frozen test/parser contract. NOT_RUN by this audit.
CREATE TABLE ch11_rows(id INTEGER PRIMARY KEY,k VARCHAR(16),v INTEGER) ORGANIZATION ORDERED(k);
INSERT INTO ch11_rows VALUES(1,'a',10),(2,'b',20),(3,'a',30);
CREATE INDEX ch11_bplus ON ch11_rows USING BPLUS(k) INCLUDE(id,v) WITH(max_keys=3);
CREATE INDEX ch11_hash ON ch11_rows USING HASH_EXTENDIBLE(k);
CREATE INDEX ch11_bitmap ON ch11_rows USING BITMAP(v);
SHOW INDEXES FROM ch11_rows;
EXPLAIN ANALYZE SELECT id,v FROM ch11_rows USE INDEX(ch11_bplus) WHERE k='a';
UPDATE ch11_rows SET k='a' WHERE id=2;
DELETE FROM ch11_rows WHERE id=1;
VERIFY;
