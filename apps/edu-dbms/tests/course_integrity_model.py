"""Deterministic differential test of immediate SQL constraints and cascading rollback."""
import argparse
import hashlib
import json
from pathlib import Path
import random
import sqlite3

from course_integrity_interfaces import FIXTURE,request,service,sha,write

def draw(rng):
    sid=rng.choice([1,2,3,4,5,6,7,8,9,9007199254740993])
    code=rng.choice(['db','os','cd']);term=rng.choice([1,2])
    grade=rng.choice(['NULL','-1','0','51','100','101'])
    return rng.choice([
      f"INSERT INTO ci_student VALUES({sid},'学生{sid}')",
      f"INSERT INTO ci_course VALUES({term},'{code}','{code}{term}')",
      f"INSERT INTO ci_enrol VALUES({sid},{term},'{code}',{grade})",
      f"UPDATE ci_enrol SET mark={grade} WHERE sid={sid}",
      f"UPDATE ci_enrol SET code='{code}' WHERE sid={sid}",
      f"DELETE FROM ci_student WHERE id={sid}",
      f"DELETE FROM ci_course WHERE term={term} AND code='{code}'",
      f"DELETE FROM ci_enrol WHERE sid={sid} AND term={term}",
    ])

def standard(rows):
    return [[str(x) if isinstance(x,int) else x for x in row] for row in rows]

QUERIES=['SELECT id,name FROM ci_student ORDER BY id','SELECT term,code,title FROM ci_course ORDER BY term,code','SELECT sid,term,code,mark FROM ci_enrol ORDER BY sid,term,code']

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--exe',type=Path,required=True);p.add_argument('--web',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--transactions',type=int,default=80)
    p.add_argument('--seeds', nargs='+', type=int, default=[42,1729,65537],
                   help='Reproducible random seeds, one independent database per seed')
    args=p.parse_args()
    if args.transactions < 1 or len(set(args.seeds)) != len(args.seeds):
        p.error('Every seed must be distinct and the transaction count must be positive')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    seed_results=[];case_spec=json.loads(FIXTURE.read_text(encoding='utf8'))
    for seed in args.seeds:
        rng=random.Random(seed);steps=[];sqlite=sqlite3.connect(':memory:',isolation_level=None)
        try:
            sqlite.execute('PRAGMA foreign_keys=ON')
            with service(args.exe.resolve(),args.web.resolve(),out/str(seed)) as (url,health):
                sid=request(url,'sessions',{})['session_id']
                def run(sql):
                    return request(url,f'sessions/{sid}/sql',{'sql':sql})
                for case in case_spec[:7]:
                    sqlite.execute(case['sql']);r=run(case['sql']);assert r['ok'],r
                for transaction in range(args.transactions):
                    sqlite.execute('BEGIN');assert run('BEGIN')['ok']
                    statements=[];failed=False
                    for _ in range(rng.randint(1,5)):
                        sql=draw(rng);r=run(sql)
                        try:sqlite.execute(sql);ok=True
                        except sqlite3.IntegrityError:ok=False
                        assert ok==r['ok'],(seed,transaction,sql,ok,r)
                        statements.append({'sql':sql,'result':'OK' if ok else r['error']['code']})
                        if not ok:
                            failed=True;assert r['transaction_state']=='aborted',r;break
                    finish='ROLLBACK' if failed or rng.random()<0.22 else 'COMMIT';sqlite.execute(finish);assert run(finish)['ok']
                    state=[]
                    for query in QUERIES:
                        expected=standard(sqlite.execute(query));observed=run(query)
                        assert observed['ok'] and observed['rows']==expected,(seed,transaction,query,expected,observed)
                        state.append(expected)
                    fingerprint=hashlib.sha256(json.dumps(state,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
                    steps.append({'transaction':transaction,'statements':statements,'ended':finish,'ordered_state_sha256':fingerprint})
                assert run('VERIFY')['ok']
        finally:sqlite.close();write(out/f'seed-{seed}.json',steps)
        seed_results.append({'seed':seed,'transactions':len(steps),'ordered_table_checks':len(steps)*len(QUERIES),'sql_mutations':sum(len(s['statements']) for s in steps)})
    write(out/'result.json',{'status':'PASS','exe_sha256':sha(args.exe),'seed_results':seed_results})
if __name__=='__main__':main()
