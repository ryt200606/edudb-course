"""Check old-writer exclusion using the actual previous release executable."""
import argparse
import hashlib
import json
import pathlib
import subprocess
from acceptance import Server, require


def hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


def main():
    parser = argparse.ArgumentParser()
    for arg in ("baseline", "candidate", "output"):
        parser.add_argument("--"+arg, type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    web = pathlib.Path(__file__).resolve().parents[1]/"web"
    old = args.output/"来自4.0"
    new = args.output/"升级至4.1"
    with Server(args.baseline.resolve(),old,web) as db:
        sid = db.session()
        db.sql(sid,"CREATE TABLE students(id INTEGER PRIMARY KEY, name VARCHAR(50))")
        db.sql(sid,"INSERT INTO students VALUES(9007199254740993,'张三'),(2,NULL)")
        before = db.sql(sid,"SELECT * FROM students ORDER BY id")["rows"]
    with Server(args.candidate.resolve(),old,web) as db:
        result = db.sql(db.session(),"ALTER TABLE students ADD CONSTRAINT positive CHECK(id>0)",ok=False)
        require(result["error"]["code"]=="CATALOG_MIGRATION_REQUIRED",str(result))
    original = hashes(old)
    run = subprocess.run([str(args.candidate.resolve()),"migrate-v4","--source",str(old),
                          "--data",str(new)],capture_output=True,timeout=120)
    require(run.returncode==0,run.stderr.decode(errors="replace"))
    migration = json.loads(run.stdout)
    require(migration["source_unchanged"] and hashes(old)==original,"source modified by migration")
    with Server(args.candidate.resolve(),new,web) as db:
        sid = db.session()
        require(before==db.sql(sid,"SELECT * FROM students ORDER BY id")["rows"],"rows changed")
        db.sql(sid,"ALTER TABLE students ADD CONSTRAINT positive CHECK(id>0)")
        failed = db.sql(sid,"INSERT INTO students VALUES(-1,'不合法')",ok=False)
    try:
        with Server(args.baseline.resolve(),new,web):
            raise AssertionError("4.0 accepted upgraded data")
    except RuntimeError as error:
        require("INCOMPATIBLE_FORMAT" in str(error),str(error))
    with Server(args.candidate.resolve(),new,web) as db:
        require(before==db.sql(db.session(),"SELECT * FROM students ORDER BY id")["rows"],"reopen differs")
    receipt = {"source_files_unchanged":True, "old_writer_rejected":True,
               "values":before, "constraint_error":failed["error"], "migration":migration}
    (args.output/"result.json").write_text(json.dumps(receipt,indent=2,ensure_ascii=False),encoding="utf8")
    print("PASS migration, constraint enforcement and old-writer exclusion")


if __name__=="__main__":
    main()
