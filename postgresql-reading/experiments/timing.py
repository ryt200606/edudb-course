#!/usr/bin/env python3
"""Repeated 14.11 query measurements without a debugger, in a fresh owned cluster."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import time

from run import Cluster, require


def dist(values):
    ordered=sorted(values)
    return {'n':len(values),'median':statistics.median(ordered),
            'p95_nearest_rank':ordered[math.ceil(.95*len(ordered))-1]}


class Timings:
    def __init__(self,cluster):
        self.cl=cluster
        self.c=cluster.connect('timing')
        self.samples=[]
        self.writes=[]

    def observe(self,sql,key,trial,rep,rows,estimate=None,fetches=None):
        start=time.perf_counter_ns()
        obj=json.loads(self.c.scalar('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql))[0]
        wall=(time.perf_counter_ns()-start)/1e6
        p=obj['Plan']
        require(p['Actual Rows']==rows and (estimate is None or p['Plan Rows']==estimate),'Predicate result or estimate mismatch')
        if fetches is not None:
            require(p['Heap Fetches']==fetches and p['Node Type']=='Index Only Scan','Unexpected heap fetch count or plan')
        self.samples.append(dict(case=key,fixture=trial,repetition=rep,node=p['Node Type'],estimated=p['Plan Rows'],
                                 actual=p['Actual Rows'],heap_fetches=p.get('Heap Fetches'),shared_hit=p['Shared Hit Blocks'],
                                 shared_read=p['Shared Read Blocks'],planning_ms=obj['Planning Time'],
                                 server_execution_ms=obj['Execution Time'],client_observed_ms=wall))

    def stats(self):
        for table,b in [('corr','i%10'),('uniform_pairs','(i/10)%10')]:
            self.c.run(f'CREATE TABLE {table}(id int PRIMARY KEY,a int,b int)')
            self.c.run(f'INSERT INTO {table} SELECT i,i%10,{b} FROM generate_series(0,999) i')
        for model in ['ordinary','mcv','dependencies']:
            queries=[]
            for table in ['corr','uniform_pairs']:
                if model!='ordinary':self.c.run(f'CREATE STATISTICS {table}_s ({model}) ON a,b FROM {table}')
                self.c.run(f'ANALYZE {table}')
                for b in [2,3]:
                    sql=f'SELECT id FROM {table} WHERE a=2 AND b={b}'
                    actual=10 if table=='uniform_pairs' else 100 if b==2 else 0
                    estimate=10 if table=='uniform_pairs' or model=='ordinary' else 1 if model=='mcv' and b==3 else 100
                    self.c.run(sql)  # warm the query, excluded from measurements
                    queries.append((sql,f'{table}/{model}/2,{b}',actual,estimate))
            for rep in range(30):
                for sql,key,actual,estimate in (queries if rep%2==0 else list(reversed(queries))):
                    self.observe(sql,key,0,rep,actual,estimate=estimate)
            if model!='ordinary':self.c.run('DROP STATISTICS corr_s, uniform_pairs_s')

    def cover(self):
        for trial in range(7):
            self.c.run('CREATE TABLE covered(id int NOT NULL,label int,payload text) WITH (fillfactor=60)')
            self.c.run("INSERT INTO covered SELECT i,i%20,repeat('x',160) FROM generate_series(1,3000) i")
            self.c.run('CREATE UNIQUE INDEX covered_idx ON covered(id) INCLUDE(label)')
            self.c.run('VACUUM (ANALYZE) covered')
            q='SELECT id,label FROM covered WHERE id='
            def measure(stage,want):
                for key in (250,2250):self.c.run(q+str(key))
                for rep in range(30):
                    for key in ((250,2250) if rep%2==0 else (2250,250)):
                        self.observe(q+str(key),f'cover/{stage}/{key}',trial,rep,1,fetches=want if key==250 else 0)
            def change(stage,sql):
                start=time.perf_counter_ns();self.c.run(sql)
                self.writes.append({'fixture':trial,'case':stage,'client_observed_ms':(time.perf_counter_ns()-start)/1e6})
            measure('clean',0)
            old=self.cl.connect('old_snapshot')
            old.run('BEGIN ISOLATION LEVEL REPEATABLE READ')
            old.run(q+'250')
            change('payload_update',"UPDATE covered SET payload=repeat('y',160) WHERE id=250")
            measure('changed',1)
            change('vacuum_while_snapshot_held','VACUUM covered')
            measure('snapshot_held',1)
            old.run('COMMIT');old.close()
            change('vacuum_after_release','VACUUM (DISABLE_PAGE_SKIPPING) covered')
            measure('after_release',0)
            self.c.run('DROP TABLE covered')

    def summary(self):
        table={}
        for key in sorted({s['case'] for s in self.samples}):
            group=[s for s in self.samples if s['case']==key]
            table[key]={col:dist([s[col] for s in group]) for col in ('planning_ms','server_execution_ms','client_observed_ms','shared_hit','shared_read')}
            table[key].update(estimated=group[0]['estimated'],actual=group[0]['actual'],heap_fetches=group[0]['heap_fetches'],
                              fixtures=len({s['fixture'] for s in group}))
        return {'queries':table,'independent_changes':{key:dist([s['client_observed_ms'] for s in self.writes if s['case']==key])
                for key in sorted({s['case'] for s in self.writes})}}


def main():
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--pg-config',default='pg_config')
    cli.add_argument('--output',type=Path,required=True)
    args=cli.parse_args()
    require(os.name=='posix' and os.geteuid()!=0,'Use a non-root POSIX account')
    args.output.mkdir(parents=True,exist_ok=False)
    cl=Cluster(args.pg_config,args.output)
    try:
        cl.init()
        run=Timings(cl)
        run.stats();run.cover()
        require(len(run.samples)==2040 and len(run.writes)==21,'Measurement set incomplete')
        for name,values in [('samples',run.samples),('changes',run.writes)]:
            with (args.output/(name+'.jsonl')).open('w',encoding='utf-8') as f:
                for entry in values:f.write(json.dumps(entry,separators=(',',':'))+'\n')
        report=run.summary()
        report.update(status='PASS',version=run.c.scalar('SELECT version()'),
                      binary_sha256=hashlib.sha256((cl.bin/'postgres').read_bytes()).hexdigest(),
                      os=platform.system()+' '+platform.release(),arch=platform.machine(),
                      shared_buffers=run.c.scalar('SHOW shared_buffers'),fsync=run.c.scalar('SHOW fsync'),
                      synchronous_commit=run.c.scalar('SHOW synchronous_commit'),
                      repeat_counts={'statistic_reads_per_case':30,'independent_coverage_fixtures':7,'reads_per_coverage_stage_per_key_per_fixture':30},
                      scope='Warm SQL, JSON EXPLAIN ANALYZE with timing; prewarm excluded; PostgreSQL execution time and end-to-end libpq time separated. No debugger or OS cache eviction.')
        (args.output/'timing.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
        print('PASS',len(run.samples),'query samples,',len(run.writes),'timed statements',flush=True)
    finally:cl.close()


if __name__=='__main__':
    main()
