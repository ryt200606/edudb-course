-- psql: run in a new, empty PostgreSQL 14.11 course database.
CREATE TABLE probe(id integer PRIMARY KEY,k integer NOT NULL,payload text NOT NULL);
INSERT INTO probe SELECT g,g%1000,repeat('x',64) FROM generate_series(1,120000) g;
CREATE INDEX probe_k_idx ON probe(k);
ANALYZE probe;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,k,payload FROM probe WHERE id BETWEEN 1 AND 114000;
EXPLAIN (ANALYZE, BUFFERS) SELECT id,k,payload FROM probe WHERE k=42;
SELECT 'probe_pkey'::regclass::oid AS observed_index_oid;
-- Run in the target backend during GDB observation.
SELECT id,k,payload FROM probe WHERE id BETWEEN 60000 AND 60020 ORDER BY id;
