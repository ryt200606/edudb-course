"""Release CLI/HTTP boundary checks; private server only, never retries SQL."""
from __future__ import annotations
import argparse, hashlib, http.client, json, pathlib, subprocess, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from acceptance import Server, require

def main():
    p=argparse.ArgumentParser();p.add_argument('--exe',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--expected-version',default='3.0.0')
    a=p.parse_args();exe=a.exe.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);checks=[]
    def check(name,fn):
        started=time.monotonic()
        try: detail=fn(); row={'name':name,'status':'PASS','detail':detail}
        except Exception as e: row={'name':name,'status':'FAIL','error':repr(e)}
        row['seconds']=round(time.monotonic()-started,3);checks.append(row)
        result={'status':'PASS' if all(x['status']=='PASS' for x in checks) else 'FAIL','binary_sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'checks':checks}
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8');print(json.dumps(row,ensure_ascii=False),flush=True)
    def cli(args,valid=False):
        r=subprocess.run([str(exe),*args],capture_output=True,timeout=8,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        require((r.returncode==0)==valid,repr((args,r.returncode,r.stdout,r.stderr)));return {'args':args,'exit':r.returncode,'output':(r.stdout+r.stderr).decode('utf8','replace')[:200]}
    check('help_version',lambda:cli(['--help'],True))
    for option,value in [('--port','0'),('--port','65536'),('--port','8765oops'),('--port','-1'),('--frames','1'),('--frames','65537'),('--frames','-1'),('--private-limit','-1'),('--private-limit','1073741825'),('--private-limit','99999999999999999999'),('--timeout-ms','0'),('--timeout-ms','600001')]:
        check('reject_'+option[2:]+'_'+value,lambda o=option,v=value:cli(['serve',o,v]))
    for name,args in [('missing_value',['serve','--port']),('unknown_mode',['nonesuch']),('unknown_option',['serve','--nonesuch','x']),('run_without_file',['run']),('missing_web',['serve','--web',str(out/'missing')])]:check(name,lambda args=args:cli(args))
    web=pathlib.Path(__file__).resolve().parents[1]/'web'
    with Server(exe,out/'中文展示 数据',web) as db:
        sid=db.session(); health=db.api('/health')
        check('health_identity',lambda:(require(health['version']==a.expected_version and health['instance_id'] in sid,'health/session identity mismatch') or health))
        def raw(method,path,body=None,headers=None):
            c=http.client.HTTPConnection('127.0.0.1',db.port,timeout=20)
            try:
                c.request(method,path,body=body,headers=headers or {});r=c.getresponse();content=r.read();return r.status,dict(r.getheaders()),content
            finally:c.close()
        def rejection(body,expected,code):
            status,headers,data=raw('POST','/api/v1/sessions/'+sid+'/sql',body,{'Content-Type':'application/json'})
            j=json.loads(data);require(status==expected and j['ok'] is False and j['error']['code']==code,repr((status,j)));return {'status':status,'code':code}
        for name,body in [('missing_sql','{}'),('null_sql','{"sql":null}'),('numeric_sql','{"sql":12}'),('array','[]'),('string','"hello"')]:check('body_'+name,lambda body=body:rejection(body,400,'INVALID_REQUEST'))
        check('invalid_json',lambda:rejection('{',400,'INVALID_JSON'))
        check('oversized_payload',lambda:rejection('x'*(1024*1024+1),413,'REQUEST_TOO_LARGE'))
        def origin(headers):
            status,_,data=raw('GET','/api/v1/health',headers=headers);j=json.loads(data);require(status==403 and j['error']['code']=='ORIGIN',repr((status,j)));return status
        check('foreign_origin',lambda:origin({'Origin':'https://foreign.invalid'}))
        check('foreign_host',lambda:origin({'Host':'foreign.invalid'}))
        def unknown():
            status,_,data=raw('GET','/api/v1/unknown');j=json.loads(data);require(status==404 and j['error']['code']=='HTTP_ERROR','404 JSON error');return status
        check('unknown_route_json',unknown)
        def defaults():
            port=db.port
            # Already-held port must fail; this private data directory has no user data.
            r=subprocess.run([str(exe),'serve','--data',str(out/'collision'),'--web',str(web),'--port',str(port)],capture_output=True,timeout=10,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            require(r.returncode==2 and b'Cannot bind' in r.stderr,'occupied port must fail clearly');return {'exit':r.returncode,'stderr':r.stderr.decode('utf8','replace')}
        check('port_collision',defaults)
        check('missing_cli_file',lambda:cli(['run','--url',db.url,'--file',str(out/'missing.sql')]))
        def rehearsal():
            q=lambda s:db.sql(sid,s)
            q('CREATE TABLE live_demo(id INTEGER PRIMARY KEY,name VARCHAR(24) NOT NULL,score REAL,grp INTEGER)')
            q("INSERT INTO live_demo VALUES(1,'张三',95,1),(2,'李四',NULL,1),(3,'王五',82.5,2)")
            q('ALTER TABLE live_demo ADD COLUMN active BOOLEAN DEFAULT TRUE');q('ALTER TABLE live_demo RENAME COLUMN name TO student')
            require(q('SELECT student FROM live_demo WHERE id=1')['rows']==[['张三']],'Chinese result')
            q('UPDATE live_demo SET score=96 WHERE id=1');q('CREATE INDEX demo_grp ON live_demo(grp)')
            plan=q('EXPLAIN ANALYZE SELECT id,student FROM live_demo WHERE id=1 LIMIT 1')
            groups=q('SELECT grp,COUNT(*) AS n,AVG(score) AS avg_score FROM live_demo GROUP BY grp HAVING COUNT(*)>0 ORDER BY grp')
            q('BEGIN');q('UPDATE live_demo SET score=0 WHERE id=1');q('ROLLBACK');require(q('SELECT score FROM live_demo WHERE id=1')['rows']==[[96.0]],'rollback')
            q('DELETE FROM live_demo WHERE id=3');q('ALTER TABLE live_demo DROP COLUMN active');require(q('SELECT COUNT(*) FROM live_demo')['rows']==[['2']],'CRUD final rows');db.close(sid)
            cp=db.api('/checkpoint','POST',{});require(cp.get('status')=='PASS' and cp.get('active_transactions')==0,repr(cp));return {'plan':plan,'groups':groups,'checkpoint':cp}
        check('complete_classroom_rehearsal',rehearsal)
        check('health_after_all_failures',lambda:db.api('/health'))
    counts={'sql_requests':0}
    class Mock(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            self.rfile.read(int(self.headers.get('Content-Length','0')))
            if self.path.endswith('/sql'):
                counts['sql_requests']+=1;time.sleep(.25)
                # Simulate response loss after execution, without a retry contract.
                self.close_connection=True;return
            body=b'{"ok":true,"session_id":"mock"}';self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def do_DELETE(self):self.send_response(200);self.send_header('Content-Length','11');self.end_headers();self.wfile.write(b'{"ok":true}')
    mock=ThreadingHTTPServer(('127.0.0.1',0),Mock);worker=threading.Thread(target=mock.serve_forever,daemon=True);worker.start()
    try:
        def lost():
            f=out/'lost-response.sql';f.write_text('INSERT INTO x VALUES(1); INSERT INTO x VALUES(2);',encoding='utf8')
            r=subprocess.run([str(exe),'run','--url',f'http://127.0.0.1:{mock.server_port}','--file',str(f),'--timeout-ms','100'],capture_output=True,timeout=6,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            require(r.returncode==1 and b'outcome may be unknown' in r.stderr and counts['sql_requests']==1,'lost write must stop without retry');return {'exit':r.returncode,'sql_requests':counts['sql_requests'],'message':r.stderr.decode()}
        check('write_response_lost_no_retry',lost)
    finally:mock.shutdown();mock.server_close();worker.join(timeout=3)
    return 0 if all(x['status']=='PASS' for x in checks) else 1
if __name__=='__main__':raise SystemExit(main())
