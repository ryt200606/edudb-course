"""Compare the same hash join under two operator budgets on a closed benchmark fixture."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from unittest.mock import patch

from acceptance import Server
from kernel_benchmark import digest, save_receipt, sha_file, sql


def resident_bytes(pid):
    if os.name == "nt":
        command = (f"$p=Get-Process -Id {int(pid)}; "
                   "@{current=$p.WorkingSet64;peak=$p.PeakWorkingSet64}|ConvertTo-Json -Compress")
        output = subprocess.check_output(["powershell.exe", "-NoProfile", "-NonInteractive",
                                          "-Command", command], timeout=15)
        return json.loads(output)
    status = Path(f"/proc/{pid}/status").read_text().splitlines()
    fields = {line.split()[0]: int(line.split()[1]) * 1024 for line in status
              if line.startswith(("VmRSS:", "VmHWM:"))}
    return {"current": fields["VmRSS:"], "peak": fields["VmHWM:"]}


def observe(args):
    saved = json.loads((args.fixture / "snapshot.json").read_bytes())
    data = args.fixture / "db"
    hashes = {p.relative_to(data).as_posix(): sha_file(p) for p in data.rglob("*") if p.is_file()}
    if hashes != saved["files"]:
        raise ValueError("Checkpoint files changed")
    labels = min(saved["identity"]["rows"], 512)
    expected = [[str(labels), f"key{labels-1:08d}-" + "q"*850, f"item{labels-1:08d}-" + "p"*128]]
    statement = ("SELECT COUNT(*),MAX(labels.tag),MAX(bench.tag) FROM bench "
                 "JOIN labels ON bench.id=labels.id USE JOIN(HASH)")
    runs = []
    for budget in (256 * 1024, 8 * 1024 * 1024):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {
                "EDUDB_QUERY_OPERATOR_MEMORY_BYTES": str(budget), "EDUDB_QUERY_TIMEOUT_MS": "240000"}):
            copy = Path(tmp) / "db"
            shutil.copytree(data, copy)
            with Server(args.exe.resolve(), copy, args.web.resolve(), frames=128, startup_timeout=180) as db:
                session = db.session()
                before = resident_bytes(db.process.pid)
                result = sql(db, session, statement)
                after = resident_bytes(db.process.pid)
                if result["rows"] != expected:
                    raise ValueError("Joined values differ from independent expectation")
                op = result["plan"]["physical_actual"]
                io = result["metrics"]["io"]["files"]["temp"]
                runs.append({"operator_budget_bytes": budget, "node": op["node"],
                             "operator_estimated_peak_bytes": op["memory_bytes_estimate_peak"],
                             "partition_passes": op.get("hash_partition_passes", 0),
                             "spill_bytes": op["spill_bytes"], "temp_read_bytes": io["read_bytes"],
                             "temp_write_bytes": io["write_bytes"], "rss_before": before,
                             "rss_after": after, "rows_sha256": digest(result["rows"])})
    small, large = runs
    if not (small["partition_passes"] > 0 and small["temp_write_bytes"] > 0
            and large["partition_passes"] == 0 and large["temp_write_bytes"] == 0):
        raise ValueError(f"Unexpected spill behavior under the two budgets: {runs}")
    if args.output.exists():
        raise FileExistsError(args.output)
    save_receipt(args.output, {"status": "PASS", "binary_sha256": sha_file(args.exe),
                               "snapshot_files": hashes, "fixture_rows": saved["identity"]["rows"],
                               "sql": statement, "runs": runs, "platform": os.name,
                               "scope": "Whole-process native RSS high-water versus the operator's estimated peak; "
                                        "one new server per budget. File calls include host cache hits."})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("exe", "fixture", "web", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    observe(parser.parse_args())
