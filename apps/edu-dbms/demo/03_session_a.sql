-- Use a persistent session A. Execute the final UPDATE after session B owns lock_b.
BEGIN;
UPDATE lock_a SET value=10 WHERE id=1;
UPDATE lock_b SET value=30 WHERE id=1;
COMMIT;
