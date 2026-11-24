"""Attribute planning and execution I/O on private copies of one benchmark checkpoint."""
import argparse
import json
from pathlib import Path
import shutil
import tempfile

from acceptance import Server
from kernel_benchmark import digest, save_receipt, sha_file, verified

def main():
    cli=argparse.ArgumentParser(description=__doc__)
    for name in ("exe","fixture","web","output"):
        cli.add_argument("--"+name,type=Path,required=True)
    args=cli.parse_args()
    snapshot=json.loads((args.fixture/"snapshot.json").read_text(encoding="utf-8"))
    n=snapshot["identity"]["rows"]
    dbfiles=args.fixture/"db"
    actual={p.relative_to(dbfiles).as_posix():sha_file(p) for p in dbfiles.rglob("*") if p.is_file()}
    if actual!=snapshot["files"]:raise ValueError("closed snapshot digest mismatch")
    lower=n//2
    cases=[("point",f"SELECT id FROM bench WHERE id={n-1}",[[str(n-1)]]),
           ("range",f"SELECT id FROM bench WHERE id>={lower} AND id<{lower+20} ORDER BY id",
            [[str(i)] for i in range(lower,lower+20)])]
    summary={"binary_sha256":sha_file(args.exe),"snapshot_files":actual,"observations":[]}
    for case,sql,expected in cases:
        with tempfile.TemporaryDirectory(dir=args.output.parent) as tmp:
            copy=Path(tmp)/"db"
            shutil.copytree(dbfiles,copy)
            with Server(args.exe.resolve(),copy,args.web.resolve(),frames=128,startup_timeout=180) as db:
                sid=db.session()
                for phase in ("autocommit","explicit_first","explicit_second"):
                    if phase=="explicit_first":db.sql(sid,"BEGIN")
                    r=verified(db.api("/sessions/"+sid+"/sql","POST",{"sql":sql},timeout=300))
                    if r["rows"]!=expected:raise AssertionError("range or point row set mismatch")
                    m=r["metrics"]
                    s=r["plan"]["sources"][0]
                    planning=m["planning_metadata_io"]
                    summary["observations"].append({"case":case,"phase":phase,"sql":sql,"rows":r["rows"],
                        "rows_sha256":digest(r["rows"]),"source":s["scan"],"index":s["index"],"estimate":s["estimated_rows"],
                        "heap_page_reads":m["heap_page_reads"],"index_page_reads":m["index_page_reads"],
                        "planning_index_page_reads":planning["index_page_reads"],
                        "execution_index_page_reads":m["index_page_reads"]-planning["index_page_reads"],
                        "row_fetches":m["row_fetches"],"cover_hits":m["cover_hits"],
                        "data_read_calls":m["io"]["files"]["data"]["read_calls"],"data_read_bytes":m["io"]["files"]["data"]["read_bytes"]})
                db.sql(sid,"ROLLBACK")
    summary["status"]="PASS"
    save_receipt(args.output,summary)
    print([(r["case"],r["phase"],r["planning_index_page_reads"],r["execution_index_page_reads"]) for r in summary["observations"]])

if __name__=="__main__":
    main()
