"""Force bounded external index construction and retain actual I/O evidence."""
from __future__ import annotations
import argparse,hashlib,importlib.util,json,os,pathlib,random,time
ROOT=pathlib.Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('edudb_acceptance',ROOT/'tests/acceptance.py')
support=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(support)

def main():
    p=argparse.ArgumentParser();p.add_argument('--exe',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--rows',type=int,default=50000);a=p.parse_args()
    if a.rows<1000 or a.rows>100000:raise ValueError('rows must be 1000..100000')
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);receipt={'status':'IN_PROGRESS','rows':a.rows,'seed':20261101,'binary_sha256':hashlib.sha256(a.exe.read_bytes()).hexdigest(),'sort_memory_bytes':262144,'checks':[]};previous=os.environ.get('EDUDB_SORT_MEMORY_BYTES');os.environ['EDUDB_SORT_MEMORY_BYTES']='262144'
    def save():(out/'result.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf8')
    try:
        with support.Server(a.exe.resolve(),out/'isolated-db',ROOT/'web',frames=16,limit=256*1024*1024) as db:
            sid=db.session()
            def q(sql):
                r=db.api('/sessions/'+sid+'/sql','POST',{'sql':sql},timeout=180);support.require(r.get('ok') is True,sql[:160]+' => '+str(r)[:500]);return r
            def verify(test,condition,details):support.require(condition,test);receipt['checks'].append({'test':test,'details':details});save()
            q('CREATE TABLE external_rows(id INTEGER PRIMARY KEY,k INTEGER,payload VARCHAR(40))')
            order=list(range(a.rows));random.Random(20261101).shuffle(order)
            for offset in range(0,a.rows,500):q('INSERT INTO external_rows VALUES '+','.join('('+str(i)+','+str(i%997)+",'row-"+str(i)+"')" for i in order[offset:offset+500]))
            before=q('SELECT COUNT(*),SUM(id),SUM(k) FROM external_rows')['rows'];start=time.monotonic();built=q('CREATE INDEX external_probe ON external_rows USING BPLUS(k) INCLUDE(id,payload)');receipt['build_seconds']=time.monotonic()-start
            inspection=q('INSPECT INDEX external_probe LIMIT 1')['inspection'];sort=inspection['structure']['descriptor']['bulk_sort'];receipt['sort']=sort;receipt['build_io']=built['metrics']['io']
            verify('real spill and streamed multiway merge',sort['input_rows']==a.rows and sort['initial_runs']>1 and sort['merge_passes']>=1 and sort['fan_in']==16 and sort['streamed_to_builder'] is True,sort)
            io=built['metrics']['io']['files']['temp'];verify('actual temporary native file I/O',io['write_bytes']>0 and io['read_bytes']>0 and io['write_calls']>1 and io['read_calls']>1,io)
            verify('owned runs removed after successful build',not list(db.data.glob('edudb-sort-*.tmp')),{'remaining':list(map(str,db.data.glob('edudb-sort-*.tmp')))})
            for k in [0,1,42,996,997]:
                actual=q('SELECT id,payload FROM external_rows USE INDEX(external_probe) WHERE k='+str(k)+' ORDER BY id')['rows'];expected=[[str(i),'row-'+str(i)] for i in range(a.rows) if i%997==k];verify('independent model key '+str(k),actual==expected,{'rows':len(actual),'sha256':hashlib.sha256(json.dumps(actual,separators=(',',':')).encode()).hexdigest()})
            failed=db.api('/sessions/'+sid+'/sql','POST',{'sql':'CREATE UNIQUE INDEX duplicate_across_runs ON external_rows USING BPLUS(k)'},timeout=180);verify('global duplicate check rejects cross-run duplicates',failed.get('ok') is False and failed['error']['code']=='UNIQUE_VIOLATION',{'error':failed.get('error'),'io':failed.get('metrics',{}).get('io',{}).get('files',{})})
            verify('owned runs removed after failed build',not list(db.data.glob('edudb-sort-*.tmp')),{'remaining':list(map(str,db.data.glob('edudb-sort-*.tmp')))})
            verify('failure preserves original rows',q('SELECT COUNT(*),SUM(id),SUM(k) FROM external_rows')['rows']==before,{'fingerprint':before})
            names=[row[0] for row in q('SHOW INDEXES FROM external_rows')['rows']];verify('failed index never published','duplicate_across_runs' not in names,{'indexes':names})
            rebuilt=q('REINDEX INDEX external_probe');verify('reindex also spills and cleans',rebuilt['metrics']['io']['files']['temp']['write_bytes']>0 and not list(db.data.glob('edudb-sort-*.tmp')),rebuilt['metrics']['io']['files']['temp'])
            q('CHECKPOINT');db.close(sid)
        with support.Server(a.exe.resolve(),out/'isolated-db',ROOT/'web',frames=4,limit=256*1024*1024) as reopened:
            s=reopened.session();actual=reopened.sql(s,'SELECT COUNT(*),SUM(id),SUM(k) FROM external_rows')['rows'];verify('checkpoint and restart preserve results',actual==before,{'fingerprint':actual});reopened.close(s)
        receipt['status']='PASS'
    except Exception as e:receipt['status']='FAIL';receipt['error']=repr(e);raise
    finally:
        if previous is None:os.environ.pop('EDUDB_SORT_MEMORY_BYTES',None)
        else:os.environ['EDUDB_SORT_MEMORY_BYTES']=previous
        save()
    print(json.dumps({'status':receipt['status'],'rows':a.rows,'checks':len(receipt['checks']),'result':str(out/'result.json')},ensure_ascii=False))
if __name__=='__main__':raise SystemExit(main())
