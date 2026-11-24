"""Exercise process exits against a durable client-side history, outside the DB."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import os
import pathlib
import threading
import urllib.error
from acceptance import Server, require

PHASES = ("wal_before_sync", "after_wal_sync", "after_page_flush",
          "checkpoint_after_data", "checkpoint_before_manifest", "checkpoint_after_manifest")


def test_phase(exe, web, root, phase):
    root.mkdir()
    journal = root / "client-history.jsonl"
    journal_lock = threading.Lock()
    data = root / "database"
    clients = 4
    args = ("--wal-group-size", "4", "--wal-group-wait-us", "10000")

    def record(client, key, state):
        with journal_lock, journal.open("a", encoding="utf8") as out:
            out.write(json.dumps([client, key, state]) + "\n")
            out.flush()
            os.fsync(out.fileno())

    def prepare(db, sid, i, key):
        db.sql(sid, "BEGIN")
        db.sql(sid, f"INSERT INTO ledger{i} VALUES ({key},1),({key},2)")

    with Server(exe, data, web, extra_args=args) as db:
        for i in range(clients):
            sid = db.session()
            db.sql(sid, f"CREATE TABLE ledger{i}(tx INTEGER, item INTEGER, UNIQUE(tx,item))")
            for key, command in [(1, "COMMIT"), (2, "ROLLBACK")]:
                prepare(db, sid, i, key)
                db.sql(sid, command)
                record(i, key, "confirmed" if key == 1 else "aborted")
            db.close(sid)

    def send_commit(i):
        barrier.wait(10)
        record(i, 3, "attempt")
        try:
            result = db.sql(sessions[i], "COMMIT", ok=None)
        except (OSError, http.client.HTTPException, urllib.error.URLError):
            record(i, 3, "unknown")
            return
        require(result.get("ok") is True, f"unexpected SQL error {result}")
        record(i, 3, "confirmed")

    with Server(exe, data, web, fault=phase, extra_args=args) as db:
        sessions = [db.session() for _ in range(clients)]
        for i, sid in enumerate(sessions):
            prepare(db, sid, i, 3)
        barrier = threading.Barrier(clients)
        with concurrent.futures.ThreadPoolExecutor(clients) as pool:
            list(pool.map(send_commit, range(clients)))
        if phase.startswith("checkpoint"):
            try:
                db.api("/checkpoint", "POST", {})
            except (OSError, http.client.HTTPException, urllib.error.URLError):
                pass
        require(db.process.wait(timeout=10) == 86, "controlled process exit did not fire")

    events = [json.loads(line) for line in journal.read_text().splitlines()]
    expected = {(i,k):status for i,k,status in events if status != "attempt"}
    fingerprints = []
    observed = {}
    for _ in range(2):
        with Server(exe, data, web, extra_args=args) as db:
            sid = db.session()
            rows = [db.sql(sid, f"SELECT tx,item FROM ledger{i} ORDER BY tx,item")["rows"]
                    for i in range(clients)]
        for i, items in enumerate(rows):
            require(all((i,int(k)) in expected for k,_ in items), "unrecorded transaction")
            for key in (1,2,3):
                parts = [int(item) for k,item in items if int(k)==key]
                status = expected[i,key]
                allowed = {"confirmed": [[1,2]], "aborted": [[]], "unknown": [[],[1,2]]}
                require(parts in allowed[status], f"{phase} {i=} {key=} {parts=} {status=}")
                observed[f"{i}/{key}"] = [status, parts]
        fingerprints.append(hashlib.sha256(json.dumps(rows).encode()).hexdigest())
    require(len(set(fingerprints)) == 1, "recovery was not idempotent")
    return {"phase":phase, "client_results":observed, "fingerprints":fingerprints}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    opts = parser.parse_args()
    opts.output.mkdir(parents=True, exist_ok=False)
    web = pathlib.Path(__file__).resolve().parents[1] / "web"
    results = []
    for phase in PHASES:
        results.append(test_phase(opts.exe.resolve(), web, opts.output/phase, phase))
        (opts.output/"results.json").write_text(json.dumps(results,indent=2),encoding="utf8")
        print(phase, results[-1]["fingerprints"][0], flush=True)
