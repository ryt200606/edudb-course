-- Use a persistent session B, begun after A. Execute final UPDATE while A waits for lock_b.
BEGIN;
UPDATE lock_b SET value=20 WHERE id=1;
UPDATE lock_a SET value=40 WHERE id=1;
-- The younger transaction is aborted by deadlock detection. ROLLBACK clears its error state.
ROLLBACK;
