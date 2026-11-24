"""Windows/process-exit acceptance across the real format-3 access methods.

No existing service or user data is used. Every process has an owned Popen
handle. Evidence and failed fixtures are retained; power-loss is not tested.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, pathlib, shutil, subprocess, time, traceback
from acceptance import Server, require

APP=pathlib.Path(__file__).resolve().parents[1]

class RecoveryServer(Server):
    # Bulk updates exercise six complete organizations with a two-frame pool.
    # This is an explicit bounded test budget, not the interactive 15s default.
    def sql(self, session, sql, ok=True):
        started=time.monotonic()
        result=self.api('/sessions/'+session+'/sql','POST',{'sql':sql},timeout=120)
        if ok is not None:require(result['ok']==ok,f'{sql}: {result}')
        print(json.dumps({'sql':sql[:160],'seconds':round(time.monotonic()-started,3),'ok':result['ok']},ensure_ascii=False),flush=True)
        return result

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def fingerprint(rows):return hashlib.sha256(json.dumps(sorted(rows,key=lambda r:int(r[0])),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
def inventories(path):return {p.relative_to(path).as_posix():sha(p) for p in path.rglob('*') if p.is_file()}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--exe',type=pathlib.Path,required=True);parser.add_argument('--v2-exe',type=pathlib.Path);parser.add_argument('--output',type=pathlib.Path,required=True);parser.add_argument('--only-migration',action='store_true');args=parser.parse_args()
    exe=args.exe.resolve();require(exe.is_file(),'Executable missing');require(args.v2_exe is None or args.v2_exe.is_file(),'Format 2 baseline executable missing');out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    result={'status':'IN_PROGRESS','binary_sha256':sha(exe),'runner_sha256':sha(pathlib.Path(__file__)),'bulk_sql_deadline_seconds':120,'fault_commit_deadline_seconds':15,'platform':'Windows' if __import__('os').name=='nt' else 'current platform','recovery_scope':'process exit and deliberately damaged files; not physical power loss','checks':[]}
    def save():(out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
    def case(name,fn):
        started=time.monotonic()
        try:details=fn();entry={'name':name,'status':'PASS','details':details}
        except Exception as error:entry={'name':name,'status':'FAIL','error':repr(error),'traceback':traceback.format_exc()}
        entry['seconds']=round(time.monotonic()-started,3);result['checks'].append(entry);save();print(json.dumps({k:v for k,v in entry.items() if k!='details'},ensure_ascii=False),flush=True)
    web=APP/'web';base=out/'base';names=['h','o','b','s','e','l']
    def setup():
        with RecoveryServer(exe,base,web,frames=4) as db:
            session=db.session()
            for name,organization in zip(names,['HEAP','ORDERED(k,id)','BPLUS(id)','HASH_STATIC(k)','HASH_EXTENDIBLE(k)','HASH_LINEAR(k)']):
                db.sql(session,f'CREATE TABLE {name}(id INTEGER PRIMARY KEY,k INTEGER,dept VARCHAR(40),flag BOOLEAN,pad VARCHAR(80),obj BLOB) ORGANIZATION {organization}')
                for start in range(0,180,60):
                    values=','.join(f"({i},{i%7},'中文{i%3}',{'TRUE' if i%2 else 'FALSE'},'payload-{i}',"+("X'"+'01'*12000+"'" if i==0 else 'NULL')+")" for i in range(start,start+60))
                    db.sql(session,'INSERT INTO '+name+' VALUES '+values)
            for method,col in [('BPLUS','dept,k'),('BTREE','k'),('ORDERED_DENSE','k'),('HASH_STATIC','k'),('HASH_EXTENDIBLE','k'),('HASH_LINEAR','k'),('BITMAP','flag')]:db.sql(session,'CREATE INDEX ix_h_'+method.lower()+' ON h USING '+method+'('+col+')')
            db.sql(session,'CREATE INDEX ix_o_sparse ON o USING ORDERED_SPARSE(k)')
            db.sql(session,"CREATE INDEX ix_b_direct ON b USING BPLUS(k) WITH(locator='direct')")
            db.sql(session,"CREATE INDEX ix_b_primary ON b USING BPLUS(k) WITH(locator='primary')")
            verification=db.sql(session,'VERIFY')['verification'];require(verification['ok'],'Initial full structural verification');db.close(session);db.api('/checkpoint','POST',{})
        with RecoveryServer(exe,base,web,frames=2) as db:
            session=db.session();old={n:db.sql(session,f'SELECT id,k,dept,flag,pad,OCTET_LENGTH(obj) FROM {n} ORDER BY id')['rows'] for n in names};db.close(session)
        (out/'expected-before.json').write_text(json.dumps(old,ensure_ascii=False,indent=2),encoding='utf8');return old
    old={} if args.only_migration else setup();new={n:[[str(int(r[0])+1000),*r[1:]] for r in rows] for n,rows in old.items()}
    for fault in ([] if args.only_migration else ['wal_before_sync','after_wal_sync','after_page_flush','checkpoint_after_data','checkpoint_before_manifest','checkpoint_after_manifest']):
        def crash(fault=fault):
            data=out/fault;shutil.copytree(base,data);checkpoint=fault.startswith('checkpoint_')
            with RecoveryServer(exe,data,web,fault=fault,frames=2) as db:
                session=db.session();db.sql(session,'BEGIN')
                for n in names:db.sql(session,f'UPDATE {n} SET id=id+1000')
                failure=None
                try:
                    if checkpoint:db.sql(session,'COMMIT');db.api('/checkpoint','POST',{},timeout=15)
                    else:db.api('/sessions/'+session+'/sql','POST',{'sql':'COMMIT'},timeout=15)
                except Exception as error:failure=type(error).__name__
                db.process.wait(timeout=20);require(db.process.returncode==86,'Expected exact owned crash fault exit')
            snapshots=[]
            for repeat in range(2):
                with RecoveryServer(exe,data,web,frames=2) as db:
                    session=db.session();actual={n:db.sql(session,f'SELECT id,k,dept,flag,pad,OCTET_LENGTH(obj) FROM {n} ORDER BY id')['rows'] for n in names};verify=db.sql(session,'VERIFY')['verification'];require(verify['ok'],'Recovered all-method page graph verified');db.close(session);snapshots.append(actual)
            require(snapshots[0]==snapshots[1],'Recovery is idempotent');require(snapshots[0] in (old,new),'Transaction cannot be partially published across organizations')
            if fault!='wal_before_sync':require(snapshots[0]==new,'Durable committed transaction was lost')
            return {'request_failure':failure,'recovered_state':'new' if snapshots[0]==new else 'old','tables':{n:fingerprint(snapshots[0][n]) for n in names},'repeat_recovery_equal':True,'verification':verify}
        case('all_access_methods_'+fault,crash)
    def private_exit():
        data=out/'uncommitted';shutil.copytree(base,data)
        with RecoveryServer(exe,data,web,frames=2) as db:
            session=db.session();db.sql(session,'BEGIN')
            for n in names:db.sql(session,f'DELETE FROM {n} WHERE id>=0')
            db.process.kill();db.process.wait(timeout=10)
        with RecoveryServer(exe,data,web,frames=2) as db:
            session=db.session();actual={n:db.sql(session,f'SELECT id,k,dept,flag,pad,OCTET_LENGTH(obj) FROM {n} ORDER BY id')['rows'] for n in names};require(actual==old,'Uncommitted pages became visible');require(db.sql(session,'VERIFY')['verification']['ok'],'Page graph after private-write crash')
        return {'uncommitted_invisible':True,'tables':len(names)}
    if not args.only_migration:case('private_pages_never_published',private_exit)
    if args.v2_exe:
        def migration():
            source=out/'v2-source';v2=args.v2_exe.resolve()
            with RecoveryServer(v2,source,web) as db:
                session=db.session();db.sql(session,'CREATE TABLE legacy(id INTEGER PRIMARY KEY,label VARCHAR(40),k INTEGER)');db.sql(session,"INSERT INTO legacy VALUES(1,'中文',7),(2,'NULL',NULL),(9223372036854775807,'最大整数',9)");db.sql(session,'CREATE INDEX legacy_k ON legacy(k)');expected=db.sql(session,'SELECT * FROM legacy ORDER BY id')['rows'];db.close(session)
                # Deliberately keep committed WAL; no checkpoint shortcuts.
            before=inventories(source);target=out/'v3-migrated'
            p=subprocess.run([str(exe),'migrate-v2','--source',str(source),'--data',str(target)],capture_output=True,timeout=120,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0));(out/'migration.stdout').write_bytes(p.stdout);(out/'migration.stderr').write_bytes(p.stderr);require(p.returncode==0,'Native migration failed: '+p.stderr.decode('utf8',errors='replace'));receipt=json.loads(p.stdout);require(receipt['status']=='PASS','Migration did not pass');require(before==inventories(source),'Migration modified source bytes')
            with RecoveryServer(exe,target,web) as db:
                session=db.session();require(db.sql(session,'SELECT * FROM legacy ORDER BY id')['rows']==expected,'Migrated values differ');require(db.sql(session,'VERIFY')['verification']['ok'],'Migrated indexes invalid')
            again=subprocess.run([str(exe),'migrate-v2','--source',str(source),'--data',str(target)],capture_output=True,timeout=20,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0));require(again.returncode!=0,'Migration overwrote an existing target');require(before==inventories(source),'Rejected migration modified source')
            return {'receipt':receipt,'source_hashes_unchanged':before,'expected_rows':expected,'old_binary_sha256':sha(v2),'existing_target_rejected':True}
        case('native_format2_readonly_migration',migration)
    result['status']='PASS' if all(c['status']=='PASS' for c in result['checks']) else 'FAIL';save();return 0 if result['status']=='PASS' else 1

if __name__=='__main__':raise SystemExit(main())
