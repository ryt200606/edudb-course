"""Replay one UTF-8 course schema through three real interfaces and an independent SQL engine."""
from __future__ import annotations
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request

FIXTURE = Path(__file__).with_name("course_integrity_workload.json")
BROWSER = Path(__file__).with_name("course_integrity_browser.cjs")

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

def request(base, route, payload=None):
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode()
    query = urllib.request.Request(base + "/api/v1/" + route, data, {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(query, timeout=20) as reply:
            return json.load(reply)
    except urllib.error.HTTPError as error:
        return json.load(error)

@contextlib.contextmanager
def service(exe, web, root):
    root.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    log = root/"server.log"
    env = {k:v for k,v in os.environ.items() if not k.startswith("EDUDB_")}
    with log.open("wb") as stream:
        process = subprocess.Popen([str(exe), "serve", "--port", str(port), "--web", str(web),
                                    "--data", str(root/"db")], env=env, stdout=stream,
                                   stderr=subprocess.STDOUT, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        try:
            deadline = time.monotonic()+25
            while time.monotonic()<deadline and process.poll() is None:
                for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
                    if not line.startswith("{"): continue
                    event=json.loads(line)
                    if event.get("event")=="listening" and event.get("url")==url:
                        health=request(url,"health")
                        if health.get("instance_id")!=event.get("instance_id"): raise RuntimeError("Server identity mismatch")
                        yield url, health
                        return
                time.sleep(.05)
            raise RuntimeError("No owned listening event: "+log.read_text(errors="replace"))
        finally:
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill();process.wait(timeout=4)

def sqlite_reference(cases):
    db=sqlite3.connect(":memory:",isolation_level=None)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        reference=[]
        for case in cases:
            try:
                rows=[list(row) for row in db.execute(case["sql"])]
            except sqlite3.IntegrityError as error:
                if "expected_error" not in case: raise
                reference.append({"case":case["case"],"sqlite_constraint":error.sqlite_errorname})
            else:
                assert "expected_error" not in case, case
                if "expected_rows" in case: assert rows==case["expected_rows"], (case,rows)
                reference.append({"case":case["case"],"rows":rows})
        return reference
    finally: db.close()

def decode_stream(text):
    decoder=json.JSONDecoder();results=[]
    while text.strip():
        value,at=decoder.raw_decode(text.lstrip());results.append(value);text=text.lstrip()[at:]
    return results

def cli_run(exe, base, root, cases):
    commands=[];remaining=[]
    def batch():
        if not remaining:return
        sql=root/f"chunk-{len(commands):02d}.sql"
        sql.write_text(";\n".join(c["sql"] for c in remaining)+";\n",encoding="utf-8")
        proc=subprocess.run([str(exe),"run","--url",base,"--file",str(sql)],capture_output=True,timeout=60)
        assert proc.returncode==(1 if "expected_error" in remaining[-1] else 0),(sql,proc.stderr)
        replies=decode_stream(proc.stdout.decode("utf-8"))
        assert len(replies)==len(remaining),(sql,len(replies))
        commands.extend(replies);remaining.clear()
    for case in cases:
        remaining.append(case)
        if "expected_error" in case: batch()
    batch()
    return commands

def compact(cases,records):
    assert len(cases)==len(records)
    evidence=[]
    for case,r in zip(cases,records):
        expected=case.get("expected_error")
        if expected:
            assert not r['ok'] and r['error']['code']==expected,(case,r)
            if 'message' in r: assert r['message'].strip(),(case,r)
        else:
            assert r['ok'],(case,r)
            if 'expected_rows' in case:
                # EduDB transports each SQL INTEGER as a decimal string.
                wanted=[[str(v) if isinstance(v,int) else v for v in row] for row in case['expected_rows']]
                assert r['rows']==wanted,(case,r['rows'])
                if 'rendered' in r:
                    cells=[["NULL" if v is None else str(v) for v in row] for row in wanted]
                    assert r['rendered']==cells,(case,r['rendered'],cells)
        evidence.append({"sql":case["sql"],"ok":r["ok"],"rows":r.get("rows"),"error":r.get("error"),"state":r.get("transaction_state"),**({"rendered":r["rendered"]} if "rendered" in r else {})})
    return evidence

def postgres_reference(cases, path):
    lines=path.read_text(encoding='utf-8-sig').splitlines()
    observations=[];active=None;rows=[]
    for line in lines:
        if line.startswith('CASE '):
            assert active is None,line
            active=int(line.split()[1]);rows=[]
        elif line.startswith('END '):
            _,case_id,sqlstate=line.split()
            assert active==int(case_id)==len(observations),line
            case=cases[active]
            assert sqlstate==case.get('pg_sqlstate','00000'),(case,sqlstate)
            if 'expected_rows' in case:assert rows==case['expected_rows'],(case,rows)
            observations.append({'case':case['case'],'sqlstate':sqlstate,'rows':rows});active=None
        elif active is not None and line.lstrip().startswith('['):
            rows.append(json.loads(line))
    assert len(observations)==len(cases) and active is None
    return observations

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--exe',type=Path,required=True);ap.add_argument('--web',type=Path,required=True)
    ap.add_argument('--output',type=Path,required=True);ap.add_argument('--node',default='node');ap.add_argument('--pg-output',type=Path)
    opts=ap.parse_args();exe=opts.exe.resolve();web=opts.web.resolve();out=opts.output.resolve();out.mkdir(parents=True,exist_ok=False)
    cases=json.loads(FIXTURE.read_text(encoding="utf8"))
    receipt={"status":"RUNNING","exe_sha256":sha(exe),"fixture_sha256":sha(FIXTURE),"case_count":len(cases),"interfaces":{}}
    try:
        write(out/'sqlite.json',sqlite_reference(cases))
        if opts.pg_output:
            write(out/'postgresql.json',postgres_reference(cases,opts.pg_output))
            receipt['postgresql']={'matched_cases':len(cases),'raw_stdout_sha256':sha(opts.pg_output)}

        for mode in ('cli','http','browser'):
            root=out/mode
            with service(exe,web,root) as (url,health):
                if mode=='cli':replies=cli_run(exe,url,root,cases)
                elif mode=='http':
                    sid=request(url,'sessions',{})['session_id']
                    replies=[request(url,f'sessions/{sid}/sql',{'sql':c['sql']}) for c in cases]
                else:
                    raw=root/'browser.json'
                    subprocess.run([opts.node,str(BROWSER),url,str(raw),str(FIXTURE)],check=True,timeout=220)
                    replies=json.loads(raw.read_text(encoding='utf-8'))
                write(root/'observations.json',compact(cases,replies))
                receipt['interfaces'][mode]={"asserted":len(replies),"version":health["version"],"instance_id":health["instance_id"],"observation_sha256":sha(root/'observations.json')}
        receipt['status']='PASS'
    except BaseException as error:
        receipt['status']='FAIL';receipt['error']=repr(error);raise
    finally: write(out/'result.json',receipt)
if __name__=='__main__': main()
