"""Independent deterministic SQL corpus and Python oracle; optional PG14 oracle.

Use --emit-corpus to send the exact same SQL to an isolated PostgreSQL cluster.
The --pg-results JSON must contain query SQL plus returned rows, not PASS labels.
"""
from __future__ import annotations
import argparse,hashlib,importlib.util,json,math,pathlib,random
SPEC=importlib.util.spec_from_file_location('support',pathlib.Path(__file__).with_name('acceptance.py'))
support=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(support)

def corpus():
 rng=random.Random(20261031);rows=[]
 for i in range(400):rows.append([i,None if i%17==0 else rng.randrange(9),None if i%11==0 else rng.randrange(-80,150),f'name-{i%13}',i%3==0])
 setup=['CREATE TABLE oracle(id INTEGER PRIMARY KEY,g INTEGER,v INTEGER,name VARCHAR(40),active BOOLEAN)']
 vals=','.join('('+','.join('NULL' if x is None else 'TRUE' if x is True else 'FALSE' if x is False else "'"+x+"'" if isinstance(x,str) else str(x) for x in r)+')' for r in rows)
 setup += ['INSERT INTO oracle VALUES '+vals,'CREATE INDEX ix_oracle_g ON oracle(g)']
 queries=[]
 def add(sql,cols,expected):queries.append({'sql':sql,'columns':cols,'expected':expected})
 add('SELECT id,v FROM oracle WHERE id>=7 AND id<31 ORDER BY id',['id','v'],[[r[0],r[2]] for r in rows if 7<=r[0]<31])
 add('SELECT id FROM oracle WHERE NOT(v<0) ORDER BY id',['id'],[[r[0]] for r in rows if r[2] is not None and r[2]>=0])
 add('SELECT id FROM oracle WHERE v IS NULL OR g IS NULL ORDER BY id',['id'],[[r[0]] for r in rows if r[2] is None or r[1] is None])
 add('SELECT id FROM oracle WHERE active=TRUE AND v>=40 ORDER BY id DESC LIMIT 13 OFFSET 2',['id'],[[r[0]] for r in reversed(rows) if r[4] and r[2] is not None and r[2]>=40][2:15])
 add('SELECT id FROM oracle WHERE id>=0 AND id=399',['id'],[[399]])
 add('SELECT id FROM oracle WHERE id=399 AND id>=0',['id'],[[399]])
 add('SELECT id FROM oracle WHERE id>300 AND id<200',['id'],[])
 groups={}
 for r in rows:groups.setdefault(r[1],[]).append(r)
 expected=[]
 for g,rs in sorted(groups.items(),key=lambda x:(x[0] is None,x[0] or 0)):
  v=[r[2] for r in rs if r[2] is not None]
  expected.append([g,len(rs),len(v),sum(v) if v else None,sum(v)/len(v) if v else None,min(v) if v else None,max(v) if v else None])
 add('SELECT g,COUNT(*) AS n,COUNT(v) AS vn,SUM(v) AS total,AVG(v) AS average,MIN(v) AS lo,MAX(v) AS hi FROM oracle GROUP BY g ORDER BY g',['g','n','vn','total','average','lo','hi'],expected)
 add('SELECT g,COUNT(*) AS n FROM oracle GROUP BY g HAVING COUNT(*)>40 ORDER BY g',['g','n'],[[r[0],r[1]] for r in expected if r[1]>40])
 add('SELECT g+1 AS bucket,COUNT(*) AS n FROM oracle GROUP BY g+1 ORDER BY bucket',['bucket','n'],[[None if r[0] is None else r[0]+1,r[1]] for r in expected])
 add('SELECT COUNT(*) AS n,SUM(v) AS total FROM oracle WHERE id<0',['n','total'],[[0,None]])
 add('SELECT g,COUNT(*) AS n FROM oracle WHERE id<0 GROUP BY g',['g','n'],[])
 add('SELECT name,COUNT(*) AS n FROM oracle GROUP BY name HAVING COUNT(*)>=30 ORDER BY name',['name','n'],[[name,sum(r[3]==name for r in rows)] for name in sorted({r[3] for r in rows}) if sum(r[3]==name for r in rows)>=30])
 add('SELECT id,name FROM oracle ORDER BY id LIMIT 0',['id','name'],[])
 return {'seed':20261031,'setup':setup,'rows':rows,'queries':queries}

def equal(actual,expected):
 if len(actual)!=len(expected):return False
 for ar,er in zip(actual,expected):
  if len(ar)!=len(er):return False
  for a,e in zip(ar,er):
   if e is None:
    if a is not None:return False
   elif isinstance(e,bool):
    if a!=e:return False
   elif isinstance(e,(int,float)):
    if a is None or not math.isclose(float(a),float(e),rel_tol=1e-12,abs_tol=1e-10):return False
   elif a!=e:return False
 return True

def main():
 p=argparse.ArgumentParser();p.add_argument('--emit-corpus',type=pathlib.Path);p.add_argument('--exe',type=pathlib.Path);p.add_argument('--output',type=pathlib.Path);p.add_argument('--pg-results',type=pathlib.Path);a=p.parse_args();c=corpus()
 if a.emit_corpus:a.emit_corpus.parent.mkdir(parents=True,exist_ok=True);a.emit_corpus.write_text(json.dumps(c,ensure_ascii=False,indent=2),encoding='utf8')
 if not a.exe:return 0
 if not a.output:raise ValueError('--output required')
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);pg=json.loads(a.pg_results.read_text('utf8')) if a.pg_results else None;checks=[]
 with support.Server(a.exe.resolve(),out/'data',pathlib.Path(__file__).resolve().parents[1]/'web') as db:
  s=db.session()
  for sql in c['setup']:db.sql(s,sql)
  for i,q in enumerate(c['queries']):
   r=db.sql(s,q['sql']);support.require(equal(r['rows'],q['expected']),'Python oracle mismatch: '+q['sql'])
   if pg:
    reference=pg['queries'][i];support.require(reference['sql']==q['sql'],'PG oracle SQL mismatch');support.require(equal(r['rows'],reference['rows']),'PostgreSQL oracle mismatch: '+q['sql'])
   checks.append({'sql':q['sql'],'actual':r['rows'],'expected':q['expected'],'plan':r.get('plan'),'metrics':r.get('metrics'),'status':'PASS'})
 result={'status':'PASS','seed':c['seed'],'queries':checks,'python_oracle':True,'postgresql_oracle':bool(pg),'binary_sha256':hashlib.sha256(a.exe.read_bytes()).hexdigest()}
 (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8');print(json.dumps({'status':'PASS','queries':len(checks),'postgresql_oracle':bool(pg)}))
if __name__=='__main__':raise SystemExit(main())
