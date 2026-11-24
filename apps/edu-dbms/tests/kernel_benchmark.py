"""Measure one pinned EduDB binary on the current OS. Uses only the standard library.

Use a new output directory for each binary/configuration. Only the owned temporary
database directories are removed. JSONL samples survive interrupted runs.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import hashlib
import json
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from acceptance import Server

SEED=20261124
BUCKETS=list(range(10))
random.Random(SEED).shuffle(BUCKETS)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def verified(result):
    if result.get("ok") is not True:
        raise RuntimeError(str(result.get("error", result))[:1500])
    return result


def sql(server, sid, text):
    return verified(server.api("/sessions/" + sid + "/sql", "POST", {"sql": text}, timeout=300))


def directory_bytes(path):
    return sum(item.stat().st_size for item in path.iterdir() if item.is_file())


def compact(result):
    metrics = result.get("metrics", {})
    plan = result.get("plan") or {}
    actual=plan.get("physical_actual") or {}
    return {"server_ms": metrics.get("elapsed_ms"), "affected": result.get("affected_rows"),
            "result_sha256": digest(result.get("rows", [])),
            "pages": {k: metrics.get(k) for k in ("heap_page_reads", "index_page_reads", "dirty_pages",
                                                   "row_fetches", "cover_hits", "wal_bytes")},
            "os_io": metrics.get("io", {}).get("files", {}),
            "sources": [{k: item[k] for k in ("scan", "estimated_rows", "estimated_access_entries",
                                              "estimated_cost","actual_rows","loops","joint_statistics_used") if k in item}
                        for item in plan.get("sources", [])],
            "join": plan.get("join_algorithm"),
            "join_counters":{k:actual[k] for k in ("node","hash_partition_passes","spill_bytes","actual_rows") if k in actual}}


def p95(values):
    return sorted(values)[math.ceil(.95 * len(values)) - 1]


@contextlib.contextmanager
def exclusive_output(path):
    handle=path.open("a+b")
    try:
        if os.name=="nt":
            import msvcrt
            if not handle.tell():handle.write(b"0");handle.flush()
            handle.seek(0);msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield
    finally:handle.close()


def sha_file(path):
    h=hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda:source.read(1024*1024),b""):h.update(block)
    return h.hexdigest()


def save_receipt(path, value):
    temp=path.with_suffix(".writing")
    temp.write_text(json.dumps(value,indent=2,ensure_ascii=False),encoding="utf-8")
    os.replace(temp,path)


def periodic_prefix(length, cycle, accepted):
    complete, tail = divmod(length, cycle)
    return complete * len(accepted) + sum(x < tail for x in accepted)


def data_rows(start, stop):
    for i in range(start, stop):
        value=BUCKETS[i%10]
        yield f"{i},{value},{value},item{i:08d}" + "-" + "p" * 128


class Benchmark:
    def __init__(self, args, temp):
        self.args, self.temp = args, temp
        self.summary = {}
        self.done=set()
        raw_path=args.output/"samples.jsonl"
        if args.resume and raw_path.exists():
            # A terminated writer may leave a partial final line. Earlier
            # malformed records are evidence corruption and must not be ignored.
            with raw_path.open("r+b") as source:
                while True:
                    offset=source.tell();line=source.readline()
                    if not line:break
                    if not line.endswith(b"\n"):
                        source.truncate(offset);break
                    item=json.loads(line)
                    key=(item["rows"],item["case"],item["repetition"])
                    if key in self.done:raise RuntimeError("duplicate saved sample "+str(key))
                    self.done.add(key)
                    self.summary.setdefault(key[:2],[]).append(item["elapsed_ms"])
        self.raw=raw_path.open("a",encoding="utf-8")

    def record(self, n, case, rep, elapsed, **details):
        if (n,case,rep) in self.done:return
        event = dict(rows=n, case=case, repetition=rep, elapsed_ms=elapsed, **details)
        print(json.dumps(event, ensure_ascii=False, separators=(",", ":")), file=self.raw, flush=True)
        self.summary.setdefault((n, case), []).append(elapsed)
        self.done.add((n,case,rep))

    def server(self, path):
        return Server(self.args.exe, path, self.args.web, frames=128, limit=32 * 1024 * 1024,extra_args=self.args.server_args,startup_timeout=180)

    def import_rows(self, server, sid, first, last):
        metrics = []
        start = time.perf_counter()
        for offset in range(first, last, self.args.batch):
            end = min(last, offset + self.args.batch)
            text = "\n".join(data_rows(offset, end)) + "\n"
            result = verified(server.api("/sessions/" + sid + "/import", "POST",
                                        {"format": "csv", "table": "bench", "header": False,
                                         "content": text}, timeout=300))
            if int(result["affected_rows"]) != end - offset:
                raise AssertionError("CSV import count")
            metrics.append(compact(result))
        return (time.perf_counter() - start) * 1000, metrics

    def ingest_fixture(self, server, sid, n, rep):
        sql(server, sid, "CREATE TABLE bench(id INTEGER PRIMARY KEY,a INTEGER,b INTEGER,tag VARCHAR(180))")
        before = directory_bytes(server.data)
        elapsed,batches=self.import_rows(server,sid,0,n)
        actual=sql(server,sid,"SELECT COUNT(*),MIN(id),MAX(id),SUM(id),SUM(a),SUM(b) FROM bench")["rows"]
        bucket_sum=n//10*sum(BUCKETS)+sum(BUCKETS[:n%10])
        if actual!=[[str(v) for v in (n,0,n-1,n*(n-1)//2,bucket_sum,bucket_sum)]]:
            raise AssertionError("full CSV import aggregate fingerprint")
        if self.args.suite=="all":
            self.record(n,"full_csv_import",rep,elapsed,imported_rows=n,
                        rows_per_second=n*1000/elapsed,batches=batches,
                        aggregate_sha256=digest(actual),growth_bytes=directory_bytes(server.data)-before)

    def fixture(self, where, n):
        with self.server(where) as server:
            sid = server.session()
            self.ingest_fixture(server,sid,n,0)
            sql(server, sid, "CREATE INDEX bench_a ON bench(a)")
            sql(server, sid, "CREATE INDEX bench_b ON bench(b)")
            if self.args.joint:
                sql(server, sid, "CREATE STATISTICS bench_ab (mcv) ON a,b FROM bench")
            sql(server, sid, "ANALYZE bench WITH (sample_rows=128,target=16)")
            sql(server, sid, "CREATE TABLE labels(id INTEGER PRIMARY KEY,tag VARCHAR(950))")
            labels = min(n, 512)
            for i in range(0, labels, 64):
                batch = ",".join(f"({j},'key{j:08d}-{'q'*850}')" for j in range(i, min(i+64, labels)))
                sql(server, sid, "INSERT INTO labels VALUES " + batch)
            for client in range(8):
                sql(server, sid, f"CREATE TABLE writer{client}(id INTEGER PRIMARY KEY)")
            count = sql(server, sid, "SELECT COUNT(*) FROM bench")["rows"]
            if int(count[0][0]) != n:
                raise AssertionError("invalid fixture")
            checkpoint=server.api("/checkpoint","POST",{})
            if checkpoint.get("status")!="PASS":raise RuntimeError(str(checkpoint))
            server.close(sid)

    def prepared_fixture(self,n):
        parent=self.args.output/"fixtures"
        parent.mkdir(exist_ok=True)
        directory=parent/str(n)
        def hashes(data):
            return {f.relative_to(data).as_posix():sha_file(f) for f in data.rglob("*") if f.is_file()}
        if directory.exists():
            saved=json.loads((directory/"snapshot.json").read_text(encoding="utf-8"))
            if hashes(directory/"db")!=saved["files"]:raise RuntimeError("fixture digest mismatch: "+str(n))
        else:
            with tempfile.TemporaryDirectory(prefix="stage-",dir=self.temp) as temp:
                stage=Path(temp)/"complete"
                stage.mkdir()
                self.fixture(stage/"db",n)
                save_receipt(stage/"snapshot.json",{"rows":n,"files":hashes(stage/"db")})
                os.replace(stage,directory)
        return directory/"db"

    @contextlib.contextmanager
    def restored(self, fixture):
        with tempfile.TemporaryDirectory(prefix="reset-", dir=self.temp) as directory:
            data = Path(directory) / "db"
            shutil.copytree(fixture, data)
            yield data

    def queries(self, n):
        point = n - 1
        lower, upper = n//2, min(n, n//2 + 20)
        labels = min(n, 512)
        return [
            ("primary_point", f"SELECT id FROM bench WHERE id={point}", [[str(point)]]),
            ("range", f"SELECT id FROM bench WHERE id>={lower} AND id<{upper} ORDER BY id",
             [[str(i)] for i in range(lower, upper)]),
            ("correlated", "SELECT COUNT(*) FROM bench WHERE a=2 AND b=2",
             [[str(periodic_prefix(n, 10, [BUCKETS.index(2)]))]]),
            ("bounded_hash_join", "SELECT COUNT(*),MAX(labels.tag),MAX(bench.tag) FROM bench "
             "JOIN labels ON bench.id=labels.id USE JOIN(HASH)",
             [[str(labels),f"key{labels-1:08d}-"+"q"*850,f"item{labels-1:08d}-"+"p"*128]])]

    def reads(self, fixture, n):
        for name, statement, expected in self.queries(n):
            # One independent restart per sample. Its immediately following run
            # has an identical input and warmed engine cache.
            for rep in range(self.args.reads):
                if all((n,name+"_"+state,rep) in self.done for state in ("reopened","warm")):continue
                with self.server(fixture) as server:
                    sid = server.session()
                    for state in ("reopened", "warm"):
                        start = time.perf_counter()
                        result = sql(server, sid, statement)
                        wall = (time.perf_counter() - start) * 1000
                        if result["rows"] != expected:
                            raise AssertionError(f"{name} result mismatch: {digest(result['rows'])} vs {digest(expected)}")
                        details = compact(result)
                        temp_bytes = details["os_io"].get("temp", {}).get("write_bytes", 0)
                        if name == "bounded_hash_join":
                            details["spill_observed"] = (temp_bytes > 0 and
                                details["join_counters"].get("hash_partition_passes",0)>0)
                            if self.args.profile == "formal" and not details["spill_observed"]:
                                raise AssertionError("The hash join did not spill at the configured memory limit")
                        self.record(n, name+"_"+state, rep, wall, sql=statement, **details)
                    server.close(sid)

    def writes(self, fixture, n):
        for rep in range(self.args.writes):
            if rep and (n,"full_csv_import",rep) not in self.done:
                with tempfile.TemporaryDirectory(prefix="ingest-",dir=self.temp) as directory:
                    with self.server(Path(directory)/"db") as server:
                        self.ingest_fixture(server,server.session(),n,rep)
            if (n,"incremental_csv",rep) not in self.done:
                with self.restored(fixture) as data, self.server(data) as server:
                    sid = server.session()
                    before = directory_bytes(data)
                    rows = min(n, 1000)
                    elapsed, batches = self.import_rows(server, sid, n, n+rows)
                    # This measures incremental, independently reset bulk imports
                    # into an already populated indexed relation.
                    result = sql(server, sid, "SELECT COUNT(*) FROM bench")
                    if int(result["rows"][0][0]) != n+rows:
                        raise AssertionError("bulk import result")
                    self.record(n, "incremental_csv", rep, elapsed, imported_rows=rows, batches=batches,
                                rows_per_second=rows*1000/elapsed, growth_bytes=directory_bytes(data)-before)
            if (n,"single_update",rep) not in self.done:
                with self.restored(fixture) as data, self.server(data) as server:
                    sid = server.session()
                    start = time.perf_counter()
                    update = sql(server, sid, f"UPDATE bench SET b=99 WHERE id={n//2}")
                    wall = (time.perf_counter() - start)*1000
                    if int(update["affected_rows"]) != 1 or sql(server,sid,f"SELECT b FROM bench WHERE id={n//2}")["rows"] != [["99"]]:
                        raise AssertionError("update result")
                    self.record(n, "single_update", rep, wall, **compact(update))
            for clients in self.args.clients:
                if (n,f"concurrent_{clients}",rep) in self.done:continue
                with self.restored(fixture) as data, self.server(data) as server:
                    self.concurrent_commits(server, clients, n, rep)
            if (n,"restart_to_verified_query",rep) in self.done:continue
            with self.restored(fixture) as data:
                with self.server(data) as server:
                    sid = server.session()
                    sql(server, sid, f"UPDATE bench SET b=99 WHERE id={n//2}")
                    sql(server, sid, "BEGIN")
                    sql(server, sid, f"UPDATE bench SET b=77 WHERE id={n//2}")
                    server.process.kill()
                    server.process.wait(timeout=10)
                start = time.perf_counter()
                with self.server(data) as server:
                    sid = server.session()
                    result = sql(server, sid, f"SELECT b FROM bench WHERE id={n//2}")
                    wall = (time.perf_counter() - start)*1000
                    if result["rows"] != [["99"]]:
                        raise AssertionError("recovery result")
                    info = server.api("/diagnostics")["storage"]
                    self.record(n, "restart_to_verified_query", rep, wall,
                                recovery={k: info.get(k) for k in ("recovered_transactions","recovered_pages","replayed_pages")},
                                **compact(result))

    def concurrent_commits(self, server, clients, n, rep):
        rounds = 8 if self.args.profile == "formal" else 2
        sessions = [server.session() for _ in range(clients)]
        barrier = threading.Barrier(clients)
        def worker(index, sid):
            latencies = []
            try:
                for sequence in range(rounds):
                    sql(server, sid, "BEGIN")
                    sql(server, sid, f"INSERT INTO writer{index} VALUES ({sequence})")
                    barrier.wait(timeout=60)
                    start = time.perf_counter()
                    sql(server, sid, "COMMIT")
                    latencies.append((time.perf_counter()-start)*1000)
            except BaseException:
                barrier.abort()
                raise
            return latencies
        before = server.api("/diagnostics")["storage"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
            start = time.perf_counter()
            latencies = list(pool.map(worker, range(clients), sessions))
            wall = (time.perf_counter()-start)*1000
        after = server.api("/diagnostics")["storage"]
        for index, sid in enumerate(sessions):
            if sql(server,sid,f"SELECT COUNT(*) FROM writer{index}")["rows"]!=[[str(rounds)]]:
                raise AssertionError("committed transaction count")
            server.close(sid)
        wal_before=before["io"]["files"]["wal"]
        wal_after=after["io"]["files"]["wal"]
        delta={key:wal_after[key]-value for key,value in wal_before.items()}
        group_a,group_b=before.get("group_commit",{}),after.get("group_commit",{})
        group={k:group_b[k]-group_a[k] for k in ("syncs","transactions") if k in group_b and k in group_a}
        group["largest_batch_at_end"]=group_b.get("largest_batch")
        self.record(n, f"concurrent_{clients}", rep, wall, clients=clients, commits=clients*rounds, group_commit=group,
                    tps=clients*rounds*1000/wall, commit_ms=latencies, wal_os_io=delta,
                    commits_per_wal_sync=(clients*rounds/delta["sync_calls"] if delta["sync_calls"] else None))

    def run(self):
        for n in self.args.sizes:
            fixture=self.prepared_fixture(n)
            if self.args.suite=="all":
                with self.restored(fixture) as fixture_read:
                    self.reads(fixture_read, n)
                self.writes(fixture, n)
            else:
                for rep in range(self.args.writes):
                    for clients in self.args.clients:
                        if (n,f"concurrent_{clients}",rep) in self.done:continue
                        with self.restored(fixture) as data,self.server(data) as server:
                            self.concurrent_commits(server,clients,n,rep)
        for n in self.args.sizes:
            writes=["concurrent_"+str(c) for c in self.args.clients]
            reads=[]
            if self.args.suite=="all":
                writes+=["full_csv_import","incremental_csv","single_update","restart_to_verified_query"]
                reads=[case+"_"+state for case,_,_ in self.queries(n) for state in ("reopened","warm")]
            for cases,count in ((writes,self.args.writes),(reads,self.args.reads)):
                for case in cases:
                    if not all((n,case,rep) in self.done for rep in range(count)):
                        raise RuntimeError("missing expected samples for "+str((n,case)))
        return [{"rows":n,"case":name,"samples":len(values),"median_ms":statistics.median(values),
                 "p95_ms":p95(values)} for (n,name),values in self.summary.items()]


def host_inventory(binary, scratch):
    info = dict(platform=platform.platform(),machine=platform.machine(),logical_cpus=os.cpu_count(),
                binary_sha256=sha_file(binary))
    for key, cmd in (("compiler_on_path",["g++","--version"]),("filesystem",["stat","-f","-c","%T",str(scratch)])):
        if key=="filesystem" and os.name=="nt":
            cmd=["powershell.exe","-NoProfile","-Command",
                 "(Get-Volume -DriveLetter "+scratch.drive[0]+").FileSystem"]
        try:
            proc=subprocess.run(cmd,capture_output=True,text=True,errors="replace",timeout=15)
            info[key]=proc.stdout.strip().splitlines()[0] if proc.returncode==0 and proc.stdout.strip() else None
        except (OSError,subprocess.TimeoutExpired):
            info[key]=None
    if hasattr(os, "sched_getaffinity"):
        info["cpu_affinity_count"]=len(os.sched_getaffinity(0))
    quota=Path("/sys/fs/cgroup/cpu.max")
    info["cgroup_cpu_max"]=quota.read_text().strip() if quota.is_file() else None
    cache=binary.parent/"CMakeCache.txt"
    if cache.is_file():
        lines=cache.read_text(encoding="utf-8",errors="replace").splitlines()
        info["binary_build"]={line.split(":",1)[0]:line.split("=",1)[1]
                              for line in lines if line.split(":",1)[0] in {
                              "CMAKE_BUILD_TYPE","CMAKE_CXX_FLAGS_RELEASE","CMAKE_CXX_FLAGS","CMAKE_CXX_COMPILER"}}
    return info


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--exe",required=True,type=Path)
    p.add_argument("--output",required=True,type=Path)
    p.add_argument("--resume",action="store_true",help="Verify the existing binary, harness and fixtures and run missing samples")
    p.add_argument("--label",required=True)
    p.add_argument("--web",type=Path,default=Path(__file__).resolve().parents[1]/"web")
    p.add_argument("--profile",choices=("smoke","formal"),default="formal")
    p.add_argument("--suite",choices=("all","group-commit"),default="all")
    p.add_argument("--sizes",type=lambda v:list(map(int,v.split(","))))
    p.add_argument("--joint",action="store_true",help="Use new MCV definition; record explicitly in comparisons")
    p.add_argument("--wal-group-size",type=int,choices=(1,8),help="Use 1 for individual syncs, 8 for group commit")
    p.add_argument("--batch",type=int,default=256)
    args=p.parse_args()
    args.server_args=[] if args.wal_group_size is None else ["--wal-group-size",str(args.wal_group_size),"--wal-group-wait-us","500"]
    formal=args.profile=="formal"
    args.sizes=args.sizes or ([1000,10000,100000] if formal else [32])
    if not args.sizes or min(args.sizes)<10 or not 1<=args.batch<=1000:
        p.error("sizes must be at least 10 and batch 1..1000")
    args.reads,args.writes=(30,7) if formal else (1,1)
    args.clients=[1,2,4,8] if formal else [1,2]
    args.exe=args.exe.resolve(strict=True)
    args.output=args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=args.resume)
    # All server children inherit this fixed operator memory budget.
    os.environ["EDUDB_QUERY_OPERATOR_MEMORY_BYTES"]="262144"
    os.environ["EDUDB_QUERY_TIMEOUT_MS"]="240000"
    info=host_inventory(args.exe,args.output)
    info.update(label=args.label,profile=args.profile,suite=args.suite,sizes=args.sizes,reads=args.reads,writes=args.writes,
                bench_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                group_environment={k:v for k,v in os.environ.items() if k.startswith("EDUDB_") and "GROUP" in k},
                server_args=args.server_args,joint_statistics=args.joint,batch=args.batch,operator_bytes=262144,buffer_frames=128,
                seed=SEED,buckets_by_id_mod_10=BUCKETS,
                fixture="fixed-seed permutation of 10 values; both predicates fully correlated; fixed-width payload",
                cache_scope="reopened server and second execution; host OS cache retained",
                restart_scope="killed owned server; restart includes readiness polling and first checked query",
                status="RUNNING")
    receipt=args.output/"receipt.json"
    with exclusive_output(args.output/"benchmark.lock"):
        if args.resume:
            prior=json.loads(receipt.read_text(encoding="utf-8"))
            for key in info:
                if key!="status" and prior.get(key)!=info[key]:raise ValueError("Cannot resume with changed "+key)
        save_receipt(receipt,info)
        try:
            with tempfile.TemporaryDirectory(dir=args.output,prefix="owned-data-") as root:
                bench=Benchmark(args,Path(root))
                try:
                    info["measurements"]=bench.run()
                    info["status"]="PASS"
                finally:bench.raw.close()
        except BaseException as error:
            info["error"]=repr(error);info["status"]="FAIL"
            raise
        finally:
            save_receipt(receipt,info)


if __name__=="__main__":
    main()
