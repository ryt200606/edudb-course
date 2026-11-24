"""Actual one-bit sort-run CRC faults; preserve database and old indexes."""
from __future__ import annotations
import argparse,hashlib,importlib.util,json,os,pathlib,shutil
ROOT=pathlib.Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('edudb_acceptance',ROOT/'tests/acceptance.py')
support=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(support)

def hashes(directory):return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(directory.iterdir()) if p.is_file()}

def main():
    p=argparse.ArgumentParser();p.add_argument('--exe',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args();out=a.output.resolve();out.mkdir(exist_ok=False,parents=True)
    saved={k:os.environ.get(k) for k in ['EDUDB_SORT_MEMORY_BYTES','EDUDB_TEST_SORT_CORRUPT']};os.environ['EDUDB_SORT_MEMORY_BYTES']='262144';os.environ.pop('EDUDB_TEST_SORT_CORRUPT',None)
    receipt={'status':'IN_PROGRESS','binary_sha256':hashlib.sha256(a.exe.read_bytes()).hexdigest(),'rows':1000,'sort_memory_bytes':262144,'checks':[]}
    def save():(out/'result.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf8')
    try:
        baseline=out/'baseline-db'
        with support.Server(a.exe.resolve(),baseline,ROOT/'web',frames=8) as db:
            s=db.session();db.sql(s,'CREATE TABLE original_rows(id INTEGER PRIMARY KEY,k INTEGER,payload VARCHAR(100))')
            db.sql(s,'INSERT INTO original_rows VALUES '+','.join('('+str(i)+','+str(i%17)+",'"+'original-'+'p'*80+"')" for i in range(1000)))
            db.sql(s,'CREATE INDEX original_index ON original_rows USING BPLUS(k) INCLUDE(id)')
            before=db.sql(s,'SELECT COUNT(*),SUM(id),SUM(k) FROM original_rows')['rows'];names=[r[0] for r in db.sql(s,'SHOW INDEXES FROM original_rows')['rows']]
            expected=db.sql(s,'SELECT id FROM original_rows USE INDEX(original_index) WHERE k=7 ORDER BY id')['rows'];db.sql(s,'CHECKPOINT');db.close(s)
        frozen=hashes(baseline);receipt['baseline_files']=frozen
        for mode in ['header','frame']:
            data=out/(mode+'-db');shutil.copytree(baseline,data);os.environ['EDUDB_TEST_SORT_CORRUPT']=mode
            with support.Server(a.exe.resolve(),data,ROOT/'web',frames=4) as db:
                s=db.session();failed=db.api('/sessions/'+s+'/sql','POST',{'sql':'CREATE INDEX corrupt_candidate ON original_rows USING BPLUS(k) INCLUDE(payload)'},timeout=60)
                support.require(failed.get('ok') is False and failed['error']['code']=='SORT_CORRUPT','Expected real sort CRC rejection: '+str(failed)[:2000])
                support.require(not list(data.glob('edudb-sort-*.tmp')),'Owned sort runs left after CRC failure')
                after=db.sql(s,'SELECT COUNT(*),SUM(id),SUM(k) FROM original_rows')['rows'];after_names=[r[0] for r in db.sql(s,'SHOW INDEXES FROM original_rows')['rows']]
                actual=db.sql(s,'SELECT id FROM original_rows USE INDEX(original_index) WHERE k=7 ORDER BY id')['rows']
                support.require(after==before and names==after_names and actual==expected,'CRC failure changed rows/schema/old index results')
                verify=db.sql(s,'VERIFY');support.require(verify['verification']['ok'] is True,'VERIFY after sort failure')
                continuation=db.sql(s,'SELECT 40+2')['rows'];support.require(continuation==[['42']],'Session did not continue after rejected index build')
                receipt['checks'].append({'fault':mode,'status':'PASS','error':failed['error'],'io':failed['metrics']['io']['files'],'fingerprint':after,'indexes':after_names,'old_index_result_sha256':hashlib.sha256(json.dumps(actual,separators=(',',':')).encode()).hexdigest(),'remaining_owned_runs':[],'verify':verify['verification']});save();db.close(s)
        support.require(hashes(baseline)==frozen,'Immutable original baseline changed');receipt['baseline_unchanged']=True;receipt['status']='PASS'
    except Exception as e:receipt['status']='FAIL';receipt['error']=repr(e);raise
    finally:
        for k,v in saved.items():
            if v is None:os.environ.pop(k,None)
            else:os.environ[k]=v
        save()
    print(json.dumps({'status':receipt['status'],'faults':len(receipt['checks']),'result':str(out/'result.json')},ensure_ascii=False))
if __name__=='__main__':raise SystemExit(main())
