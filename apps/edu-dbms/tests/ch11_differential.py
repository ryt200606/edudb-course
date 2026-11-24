"""Independent SQLite result oracle for chapter-11 SQL access paths.

Fresh directories only; no desktop, external DB, user data, or timing claims.
"""
from __future__ import annotations
import argparse, hashlib, importlib.util, json, math, pathlib, random, sqlite3, sys

ROOT=pathlib.Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('edudb_acceptance',ROOT/'tests/acceptance.py')
support=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(support)

def digest(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()

def literal(value):
    if value is None:return 'NULL'
    if isinstance(value,bool):return 'TRUE' if value else 'FALSE'
    if isinstance(value,str):return "'"+value.replace("'","''")+"'"
    return repr(value)

def typed_rows(result):
    values=[]
    for row in result['rows']:
        out=[]
        for c,v in zip(result['columns'],row):
            if v is not None and c['type']=='INTEGER':v=int(v)
            if isinstance(v,bool):v=int(v)
            out.append(v)
        values.append(out)
    return values

def equal_rows(actual,expected):
    if len(actual)!=len(expected):return False
    for a,b in zip(actual,expected):
        if len(a)!=len(b):return False
        for x,y in zip(a,b):
            if isinstance(x,(int,float)) and isinstance(y,(int,float)):
                if isinstance(x,int) and isinstance(y,int):
                    if x!=y:return False
                elif not math.isclose(x,y,rel_tol=1e-12,abs_tol=1e-12):return False
            elif type(x)!=type(y) or x!=y:return False
    return True

def main():
    p=argparse.ArgumentParser();p.add_argument('--exe',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--seed',type=int,default=20261101);args=p.parse_args()
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False);rng=random.Random(args.seed)
    receipt={'status':'IN_PROGRESS','seed':args.seed,'binary_sha256':hashlib.sha256(args.exe.read_bytes()).hexdigest(),'oracle':'Python sqlite3 in-memory connection; typed NULL/text/integer exact; finite REAL tolerance 1e-12','queries':[],'methods':[]}
    def save(): (out/'result.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf8')
    try:
        with support.Server(args.exe.resolve(),out/'isolated-db',ROOT/'web',frames=16) as db:
            sid=db.session();oracle=sqlite3.connect(':memory:')
            def q(sql):
                result=db.sql(sid,sql);support.require(result.get('ok') is True,sql+' => '+repr(result));return result
            def both(sql):q(sql);oracle.execute(sql)
            def compare(sql,reference=None,force=None):
                r=q(sql);expected=[list(row) for row in oracle.execute(reference or sql)];actual=typed_rows(r)
                if not equal_rows(actual,expected):
                    receipt['mismatch']={'sql':sql,'actual':actual,'expected':expected};raise AssertionError('Oracle mismatch: '+sql)
                plan=r.get('plan') or {};source=(plan.get('sources') or [{}])[0]
                if force is not None:support.require(source.get('index')==force and source.get('scan')!='SeqScan','Forced path not executed: '+repr(source))
                receipt['queries'].append({'sql':sql,'reference_sql':reference or sql,'rows':len(actual),'result_sha256':digest(actual),'scan':source.get('scan'),'index':source.get('index'),'plan_node':plan.get('node'),'metrics':{k:v for k,v in r.get('metrics',{}).items() if k!='io'}})
            schema='(id INTEGER PRIMARY KEY,k INTEGER,g VARCHAR(40),v REAL,b BOOLEAN)'
            q('CREATE TABLE r '+schema+' ORGANIZATION ORDERED(k)');oracle.execute('CREATE TABLE r '+schema)
            dataset=[]
            for i in range(128):dataset.append([i,None if i%11==0 else i%9,None if i%13==0 else ['中文甲','中文乙',"O'Neil",''][i%4],None if i%17==0 else (i-60)/8,None if i%7==0 else bool(i%2)])
            insert='INSERT INTO r VALUES '+','.join('('+','.join(map(literal,row))+')' for row in dataset);both(insert)
            methods=[('BPLUS',''),('BTREE',''),('DENSE',''),('SPARSE',''),('MULTILEVEL',''),('HASH_STATIC',' WITH (buckets=8)'),('HASH_EXTENDIBLE',' WITH (max_keys=7)'),('HASH_LINEAR',' WITH (buckets=2,max_keys=7)'),('BITMAP',' WITH (hybrid=FALSE)'),('BITMAP_HYBRID',' WITH (bitmap_threshold=4)'),('BPLUS',' WITH (dup_layout=\'posting\',max_keys=7)'),('HASH_STATIC',' WITH (probing=TRUE,buckets=8,max_keys=31)'),('BPLUS',' WITH (bstar=TRUE,max_keys=7)')]
            for number,(method,options) in enumerate(methods):
                if method in ['SPARSE','MULTILEVEL']:q('ALTER TABLE r ORGANIZATION ORDERED(k) WITH(multilevel='+('FALSE' if method=='SPARSE' else 'TRUE')+')')
                index='candidate_'+str(number);q('CREATE INDEX '+index+' ON r USING '+method+'(k)'+options)
                for _ in range(20):
                    key=rng.randrange(-2,12);ref='SELECT id,g,v,b FROM r WHERE k='+str(key)+' ORDER BY id';compare(ref.replace('FROM r','FROM r USE INDEX('+index+')'),ref,index)
                for condition in ['k IS NULL','k IS NOT NULL','k=NULL']:
                    ref='SELECT id FROM r WHERE '+condition+' ORDER BY id'
                    if condition=='k IS NULL':compare(ref.replace('FROM r','FROM r USE INDEX('+index+')'),ref,index)
                    else:compare(ref)
                for _ in range(12):
                    low=rng.randrange(-2,10);high=low+rng.randrange(0,6)
                    compare('SELECT COUNT(*),SUM(v),MIN(id),MAX(id) FROM r WHERE k>='+str(low)+' AND k<'+str(high))
                # Structural maintenance under real SQL, with the independent
                # oracle receiving the same operations and retained state.
                for _ in range(6):
                    target=rng.randrange(128);key=rng.randrange(9);both('UPDATE r SET k='+str(key)+' WHERE id='+str(target))
                    ref='SELECT id FROM r WHERE k='+str(key)+' ORDER BY id';compare(ref.replace('FROM r','FROM r USE INDEX('+index+')'),ref,index)
                q('REINDEX INDEX '+index)
                compare('SELECT id,k,g,v,b FROM r ORDER BY id')
                receipt['methods'].append({'input':method,'options':options,'queries_complete':len(receipt['queries'])});q('DROP INDEX '+index);save()
            q('CREATE INDEX r_bitmap_k ON r USING BITMAP(k)');q('CREATE INDEX r_bitmap_g ON r USING BITMAP(g)');q('CREATE INDEX r_bitmap_b ON r USING BITMAP(b)')
            for condition in ["k=1 AND g='中文甲'","NOT(k=1)","NOT(k=1 OR k IS NULL)","(k=1 OR g='中文乙') AND NOT(b=TRUE)","g IS NULL OR b IS NULL","NOT(g IS NULL) AND b IS NOT NULL","(k=1 AND b=FALSE) OR (g='中文甲' AND b=TRUE)"]:
                compare('SELECT id FROM r WHERE '+condition+' ORDER BY id');compare('SELECT COUNT(*) FROM r WHERE '+condition)
            both('DELETE FROM r WHERE id<16');both("INSERT INTO r VALUES(900,7,'复用',1.25,TRUE),(901,NULL,NULL,NULL,NULL)")
            compare('SELECT COUNT(*) FROM r');compare('SELECT id FROM r WHERE NOT(k=1) AND b IS NOT NULL ORDER BY id')
            q('CREATE TABLE intersect_rows(id INTEGER PRIMARY KEY,k INTEGER,g VARCHAR(8),padding VARCHAR(1200))');oracle.execute('CREATE TABLE intersect_rows(id INTEGER PRIMARY KEY,k INTEGER,g VARCHAR(8),padding VARCHAR(1200))')
            both('INSERT INTO intersect_rows VALUES '+','.join('('+str(i)+','+str(i%8)+",'g"+str(i%5)+"','"+'p'*1000+"')" for i in range(128)))
            q('CREATE INDEX inter_k ON intersect_rows(k)');q('CREATE INDEX inter_g ON intersect_rows(g)')
            compare("SELECT id FROM intersect_rows WHERE k=2 AND g='g2' ORDER BY id")
            compare("SELECT id FROM intersect_rows USE SCAN WHERE k=2 AND g='g2' ORDER BY id","SELECT id FROM intersect_rows WHERE k=2 AND g='g2' ORDER BY id")
            receipt['oracle_sqlite_version']=sqlite3.sqlite_version;receipt['final_row_count']=oracle.execute('SELECT COUNT(*) FROM r').fetchone()[0];oracle.close();db.close(sid)
        receipt['status']='PASS';receipt['query_count']=len(receipt['queries']);receipt['results_sha256']=digest(receipt['queries'])
    except Exception as error:receipt['status']='FAIL';receipt['error']=repr(error);raise
    finally:save()
    print(json.dumps({'status':receipt['status'],'queries':receipt['query_count'],'methods':len(receipt['methods']),'result':str(out/'result.json')},ensure_ascii=False))
if __name__=='__main__':raise SystemExit(main())
