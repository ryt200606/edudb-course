"""Independent HTTP/CLI, concurrency and process-crash acceptance. Keeps all evidence."""
from __future__ import annotations
import argparse, concurrent.futures, datetime, hashlib, json, os, pathlib, socket
import subprocess, sys, threading, time, urllib.error, urllib.request

def require(ok, message):
    if not ok: raise AssertionError(message)

class Server:
    def __init__(self, exe, data, web, fault=None, limit=33554432, frames=4, extra_args=(), startup_timeout=25):
        self.data=pathlib.Path(data);self.data.mkdir(parents=True,exist_ok=True)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1]
        self.url=f'http://127.0.0.1:{self.port}'
        self.log_path=self.data.parent/(self.data.name+'-'+(fault or 'normal')+'-'+str(time.time_ns())+'.log')
        self.log=self.log_path.open('wb');env=os.environ.copy()
        if fault:env['EDUDB_CRASH_AT']=fault
        else:env.pop('EDUDB_CRASH_AT',None)
        cmd=[str(exe),'serve','--data',str(self.data),'--web',str(web),'--port',str(self.port),'--frames',str(frames),'--private-limit',str(limit)]
        cmd.extend(extra_args)
        self.process=subprocess.Popen(cmd,stdout=self.log,stderr=subprocess.STDOUT,env=env,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        deadline=time.monotonic()+startup_timeout
        try:
            while time.monotonic()<deadline:
                if self.process.poll() is not None:raise RuntimeError('Server exited: '+self.log_path.read_text(encoding='utf8',errors='replace'))
                # A free-port probe has a race. Require this owned child's
                # successful-bind receipt before sending any mutable request.
                bound=None
                for line in self.log_path.read_text(encoding='utf8',errors='replace').splitlines():
                    try:event=json.loads(line)
                    except json.JSONDecodeError:continue
                    if event.get('event')=='listening' and event.get('url')==self.url:bound=event
                if bound and bound.get('instance_id'):
                    try:
                        health=self.api('/health')
                        if health.get('ok') and health.get('instance_id')==bound['instance_id']:
                            require(self.process.poll() is None,'Owned server exited during readiness')
                            self.instance_id=bound['instance_id'];return
                    except (OSError,urllib.error.URLError):pass
                time.sleep(.04)
            raise TimeoutError('Owned server successful-bind/identity readiness timeout')
        except BaseException:
            self.stop();raise
    def api(self,path,method='GET',body=None,timeout=15):
        payload=None if body is None else json.dumps(body,ensure_ascii=False).encode('utf8')
        req=urllib.request.Request(self.url+'/api/v1'+path,data=payload,method=method,headers={'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(req,timeout=timeout) as response:return json.load(response)
        except urllib.error.HTTPError as error:
            try:return json.load(error)
            except ValueError:raise error
    def session(self):return self.api('/sessions','POST',{})['session_id']
    def sql(self,session,sql,ok=True):
        result=self.api('/sessions/'+session+'/sql','POST',{'sql':sql})
        if ok is not None:require(result['ok']==ok,f'{sql}: {result}')
        return result
    def close(self,s):return self.api('/sessions/'+s,'DELETE')
    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=5)
        self.log.close()
    def __enter__(self):return self
    def __exit__(self,*args):self.stop()

def wait_for(predicate,description,seconds=4):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        value=predicate()
        if value:return value
        time.sleep(.015)
    raise TimeoutError(description)

def main():
    p=argparse.ArgumentParser();p.add_argument('--exe',required=True);p.add_argument('--output',required=True);a=p.parse_args()
    exe=pathlib.Path(a.exe).resolve();out=pathlib.Path(a.output).resolve();out.mkdir(parents=True,exist_ok=False)
    web=pathlib.Path(__file__).resolve().parents[1]/'web';results=[]
    def case(name,function):
        start=time.monotonic()
        try:details=function();result=dict(name=name,status='PASS',details=details)
        except Exception as e:result=dict(name=name,status='FAIL',error=repr(e))
        result['seconds']=round(time.monotonic()-start,3);results.append(result);print(json.dumps({k:v for k,v in result.items() if k!='details'},ensure_ascii=False),flush=True)
        (out/'result.json').write_text(json.dumps(dict(status='PASS' if all(x['status']=='PASS' for x in results) else 'FAIL',checks=results),ensure_ascii=False,indent=2),encoding='utf8')

    def sql_features():
        with Server(exe,out/'中文 空格 CRUD',web) as db:
            s=db.session();q=lambda x,ok=True:db.sql(s,x,ok)
            q('CREATE TABLE students(id INTEGER PRIMARY KEY, name VARCHAR(8) NOT NULL, score REAL, active BOOLEAN DEFAULT TRUE)')
            q("INSERT INTO students(id,name,score) VALUES(1,'张三',90),(2,'李四',NULL),(3,'NULL',75)")
            require(q('SELECT name FROM students WHERE score IS NULL')['rows']==[['李四']],'NULL predicate')
            require(q('SELECT id FROM students WHERE NOT (score < 80) ORDER BY id')['rows']==[['1']],'three-valued NOT')
            require(q('SELECT id FROM students WHERE active = TRUE ORDER BY id DESC LIMIT 2')['rows']==[['3'],['2']],'boolean sort limit')
            q('INSERT INTO students VALUES(1,\'重复\',0,TRUE)',False);q("INSERT INTO students VALUES(4,NULL,0,TRUE)",False)
            q("INSERT INTO students VALUES(4,'太长的中文字符串超过八字',0,TRUE)",False)
            q('UPDATE students SET score = 95 WHERE id = 1');q('DELETE FROM students WHERE id = 3')
            q('BEGIN');q("UPDATE students SET name='临时' WHERE id=1");q('ROLLBACK')
            require(q('SELECT name FROM students WHERE id=1')['rows']==[['张三']],'rollback')
            q('ALTER TABLE students ADD COLUMN note VARCHAR(20) DEFAULT \'默认\'')
            q('ALTER TABLE students RENAME COLUMN note TO memo')
            require(q('SELECT memo FROM students ORDER BY id')['rows']==[['默认'],['默认']],'ALTER old rows')
            q('ALTER TABLE students DROP COLUMN memo');q('ALTER TABLE students RENAME TO people')
            q('BEGIN');q('ALTER TABLE people ADD COLUMN extra INTEGER DEFAULT 7');q('ROLLBACK')
            require(len(q('SELECT * FROM people')['columns'])==4,'transactional DDL rollback')
            q('ALTER TABLE people DROP COLUMN id',False)
            q('BEGIN');q('INSERT INTO people VALUES(1,\'dup\',0,TRUE)',False)
            require(q('SELECT * FROM people',False)['error']['code']=='TRANSACTION_ABORTED','aborted transaction')
            q('ROLLBACK');q("INSERT INTO people VALUES(9223372036854775807,'大整数',0,TRUE)")
            require(q('SELECT id FROM people WHERE id=9223372036854775807')['rows']==[['9223372036854775807']],'integer wire precision')
            q('CREATE TABLE grades(id INTEGER PRIMARY KEY, mark INTEGER)');q('INSERT INTO grades VALUES(1,80),(2,90)')
            rows=q('SELECT p.name,g.mark FROM people AS p INNER JOIN grades AS g ON p.id=g.id ORDER BY g.mark')['rows']
            require(rows==[['张三','80'],['李四','90']],'inner join')
            require(q('SELECT COUNT(*),SUM(mark),AVG(mark),MIN(mark),MAX(mark) FROM grades')['rows'][0]==['2','170',85.0,'80','90'],'aggregates')
            with urllib.request.urlopen(db.url) as response:require('EduDB' in response.read().decode('utf8'),'web root')
            bad=urllib.request.Request(db.url+'/api/v1/health',headers={'Origin':'http://evil.example'})
            try:urllib.request.urlopen(bad);raise AssertionError('Cross-origin request accepted')
            except urllib.error.HTTPError as e:require(e.code==403,'origin status')
            db.close(s);diag=db.api('/diagnostics')
        with Server(exe,out/'中文 空格 CRUD',web) as recovered:
            require(recovered.sql(recovered.session(),'SELECT COUNT(*) FROM people')['rows']==[['3']],'restart persistence')
        return {'sql_cases':'DDL/CRUD/NULL/UTF8/constraints/joins/aggregates/int64/HTTP/restart','diagnostics':diag}
    case('sql_and_http',sql_features)

    def cli_script():
        with Server(exe,out/'cli-service',web) as db:
            sqlfile=out/'带空格 演示.sql'
            sqlfile.write_text("-- comment; must not split\nCREATE TABLE cli(id INTEGER PRIMARY KEY,txt VARCHAR(80));\nINSERT INTO cli VALUES(1,'a;b''c');\nSELECT txt FROM cli;\nSELECT 'NULL';\n-- trailing comment only\n",encoding='utf8')
            run=subprocess.run([str(exe),'run','--url',db.url,'--file',str(sqlfile)],capture_output=True,timeout=20,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            (out/'cli-output.json.txt').write_bytes(run.stdout+run.stderr)
            require(run.returncode==0,'CLI script failed: '+run.stdout.decode('utf8',errors='replace'))
            require("a;b'c" in run.stdout.decode('utf8'),'CLI quoted semicolon handling')
            failing=out/'stop-on-error.sql';failing.write_text('SELECT missing FROM cli; INSERT INTO cli VALUES(2,\'should not run\');',encoding='utf8')
            stopped=subprocess.run([str(exe),'run','--url',db.url,'--file',str(failing)],capture_output=True,timeout=15,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            require(stopped.returncode==1,'CLI did not fail on first error')
            require(db.sql(db.session(),'SELECT COUNT(*) FROM cli')['rows']==[['1']],'CLI continued after error')
            return {'script_exit':run.returncode,'error_exit':stopped.returncode,'unicode_file_path':True}
    case('cli_unicode_script_and_error_stop',cli_script)

    def index_and_buffer():
        with Server(exe,out/'index-buffer',web,frames=2) as db:
            s=db.session();db.sql(s,'CREATE TABLE samples(id INTEGER PRIMARY KEY,k INTEGER,label VARCHAR(80))')
            values=','.join(f"({i},{i%7},'value-{i:04d}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')" for i in range(700))
            db.sql(s,'INSERT INTO samples VALUES '+values);db.sql(s,'CREATE INDEX ix_samples_k ON samples(k)')
            for op,key in [('=',3),('>',4),('<=',1),('>=',5),('<',2)]:
                pred={'=':lambda k:k==key,'>':lambda k:k>key,'<=':lambda k:k<=key,'>=':lambda k:k>=key,'<':lambda k:k<key}[op]
                rows=db.sql(s,f'SELECT id FROM samples WHERE k {op} {key} ORDER BY id')['rows']
                require(rows==[[str(i)] for i in range(700) if pred(i%7)],'persistent B+ tree range '+op)
            plan=db.sql(s,'EXPLAIN SELECT * FROM samples WHERE k=3')['plan']
            require('index' in json.dumps(plan).lower(),'EXPLAIN actual index selection')
            db.sql(s,'UPDATE samples SET k=9 WHERE id<50');db.sql(s,'DELETE FROM samples WHERE id>=650')
            require(db.sql(s,'SELECT COUNT(*) FROM samples WHERE k=9')['rows']==[['50']],'updated index')
            db.sql(s,'DROP INDEX ix_samples_k')
            require(db.sql(s,'SELECT COUNT(*) FROM samples WHERE k=9')['rows']==[['50']],'scan/index equivalence')
            diag=db.api('/diagnostics');db.close(s)
        return {'rows_inserted':700,'buffer_frames':2,'diagnostics':diag}
    case('persistent_index_and_eviction',index_and_buffer)

    def concurrency():
        with Server(exe,out/'concurrency',web) as db:
            a,b=db.session(),db.session()
            db.sql(a,'CREATE TABLE aa(id INTEGER PRIMARY KEY,v INTEGER)');db.sql(a,'CREATE TABLE bb(id INTEGER PRIMARY KEY,v INTEGER)')
            db.sql(a,'INSERT INTO aa VALUES(1,10)');db.sql(a,'INSERT INTO bb VALUES(1,20)')
            db.sql(a,'BEGIN');db.sql(b,'BEGIN');db.sql(a,'UPDATE aa SET v=11 WHERE id=1');db.sql(b,'UPDATE bb SET v=21 WHERE id=1')
            diag=db.api('/diagnostics');db.sql(a,'COMMIT');db.sql(b,'ROLLBACK')
            require(db.sql(a,'SELECT v FROM aa')['rows']==[['11']],'different table commit')
            require(db.sql(a,'SELECT v FROM bb')['rows']==[['20']],'different table rollback')
            db.sql(a,'BEGIN');db.sql(b,'BEGIN');db.sql(a,'SELECT * FROM aa');db.sql(b,'SELECT * FROM aa');db.sql(b,'COMMIT')
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                started=threading.Event()
                def writer():started.set();return db.sql(b,'UPDATE aa SET v=12 WHERE id=1')
                f=pool.submit(writer);started.wait(1)
                wait_for(lambda:db.api('/diagnostics').get('waits'),'read/write lock wait not observed')
                require(not f.done(),'writer bypassed shared lock');db.sql(a,'COMMIT');require(f.result(timeout=7)['ok'],'writer completion')
            db.sql(a,'BEGIN');db.sql(b,'BEGIN');db.sql(a,'UPDATE aa SET v=13 WHERE id=1');db.sql(b,'UPDATE bb SET v=22 WHERE id=1')
            with concurrent.futures.ThreadPoolExecutor(2) as pool:
                f=pool.submit(db.sql,a,'UPDATE bb SET v=23 WHERE id=1',None)
                wait_for(lambda:db.api('/diagnostics').get('waits'),'first deadlock edge missing')
                g=pool.submit(db.sql,b,'UPDATE aa SET v=14 WHERE id=1',None)
                rs=[f.result(timeout=7),g.result(timeout=7)]
                require(sum(not x['ok'] for x in rs)==1,'one deadlock victim required')
                require(any((x.get('error') or {}).get('code')=='DEADLOCK' for x in rs),'deadlock detection required')
            db.sql(a,'COMMIT' if rs[0]['ok'] else 'ROLLBACK');db.sql(b,'COMMIT' if rs[1]['ok'] else 'ROLLBACK')
            db.sql(a,'BEGIN');db.sql(a,'UPDATE aa SET v=99 WHERE id=1');db.close(a)
            require(db.sql(b,'SELECT v FROM aa')['rows']!=[['99']],'session close rollback')
            db.close(b)
            return {'different_table_overlap_snapshot':diag,'deadlock_results':rs}
    case('strict_2pl_and_deadlock',concurrency)

    def limits_and_lock():
        with Server(exe,out/'limits',web,limit=32768) as db:
            s=db.session();db.sql(s,'CREATE TABLE t(id INTEGER PRIMARY KEY,txt VARCHAR(1000))')
            values=','.join(f"({i},'{('x'*700)}')" for i in range(80))
            failure=db.sql(s,'INSERT INTO t VALUES '+values,False)
            require(db.sql(s,'SELECT COUNT(*) FROM t')['rows']==[['0']],'private limit partial mutation')
            duplicate=subprocess.run([str(exe),'serve','--data',str(db.data),'--port',str(db.port+1),'--web',str(web)],capture_output=True,timeout=10,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            require(duplicate.returncode!=0,'second process opened datadir')
            return {'resource_error':failure['error'],'second_process_exit':duplicate.returncode}
    case('resource_limit_and_directory_lock',limits_and_lock)

    def checkpoint_busy():
        with Server(exe,out/'checkpoint-busy',web) as db:
            s=db.session();db.sql(s,'BEGIN');start=time.monotonic();result=db.api('/checkpoint','POST',{})
            require(time.monotonic()-start<8,'checkpoint wait unbounded')
            require('BUSY' in json.dumps(result).upper(),'checkpoint did not report BUSY')
            db.sql(s,'ROLLBACK');return {'busy':result,'completed':db.api('/checkpoint','POST',{})}
    case('checkpoint_bounded_wait',checkpoint_busy)

    for fault in ['wal_before_sync','after_wal_sync','after_page_flush','checkpoint_after_data','checkpoint_before_manifest','checkpoint_after_manifest']:
        def crash(fault=fault):
            data=out/('crash-'+fault)
            with Server(exe,data,web,frames=2) as db:
                s=db.session();db.sql(s,'CREATE TABLE t(id INTEGER PRIMARY KEY,v INTEGER)');db.sql(s,'INSERT INTO t VALUES(1,10)')
                if fault.startswith('checkpoint_'):db.sql(s,'INSERT INTO t VALUES(2,20)')
            db=Server(exe,data,web,fault=fault,frames=2)
            try:
                try:
                    if fault.startswith('checkpoint_'):db.api('/checkpoint','POST',{})
                    else:db.sql(db.session(),'INSERT INTO t VALUES(2,20)',None)
                except (OSError,ValueError):pass
                db.process.wait(timeout=10);require(db.process.returncode==86,'fault point not reached')
            finally:db.stop()
            with Server(exe,data,web,frames=2) as recovered:
                rows=recovered.sql(recovered.session(),'SELECT id,v FROM t ORDER BY id')['rows']
                expected=[['1','10'],['2','20']]
                if fault=='wal_before_sync':require(rows in [[['1','10']],expected],'pre-sync result not atomic')
                else:require(rows==expected,'durable commit missing '+fault)
            with Server(exe,data,web,frames=2) as again:
                require(again.sql(again.session(),'SELECT id,v FROM t ORDER BY id')['rows']==rows,'redo not idempotent')
            return {'fault':fault,'exit_code':86,'recovered_rows':rows,'second_recovery':'same'}
        case('crash_'+fault,crash)

    summary={'status':'PASS' if all(r['status']=='PASS' for r in results) else 'FAIL','platform':sys.platform,
        'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary':str(exe),'binary_sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),
        'guarantee':'process-failure recovery only; no physical power-loss claim','checks':results}
    (out/'result.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps({'status':summary['status'],'passed':sum(r['status']=='PASS' for r in results),'total':len(results),'result':str(out/'result.json')},ensure_ascii=False))
    return 0 if summary['status']=='PASS' else 1
if __name__=='__main__':raise SystemExit(main())
