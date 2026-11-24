#!/usr/bin/env python3
"""Exercise four PostgreSQL 14.11 mechanisms in an owned, socket-only cluster."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

from client import Connection, Libpq, SqlError


def require(ok, msg):
    if not ok:raise RuntimeError(msg)


def until(predicate, msg, seconds=4):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if predicate():return
        time.sleep(.02)
    raise RuntimeError(msg)


class Cluster:
    def __init__(self, pg_config, output):
        self.out=output
        self.env={k:v for k,v in os.environ.items() if not k.startswith('PG')}
        self.env['LC_ALL']='C'
        self.bin=Path(self.cmd([pg_config,'--bindir']).strip())
        version=self.cmd([str(self.bin/'postgres'),'--version']).strip()
        require(version=='postgres (PostgreSQL) 14.11','PostgreSQL 14.11 tools required, found '+version)
        libdir=Path(self.cmd([pg_config,'--libdir']).strip())
        so=next((p for p in [libdir/'libpq.so.5',libdir/'libpq.5.dylib',libdir/'libpq.dylib'] if p.is_file()),None)
        require(so,'libpq not found in the selected PostgreSQL installation')
        self.pq=Libpq(so)
        self.root=Path(tempfile.mkdtemp(prefix='pg14-course-',dir='/tmp'))
        self.data,self.sock=self.root/'data',self.root/'socket'
        self.sock.mkdir(mode=0o700)
        self.port=54327
        self.clients=[]

    def cmd(self,args):
        return subprocess.run(list(map(str,args)),check=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                              env=self.env,text=True,encoding='utf-8',errors='replace',timeout=65).stdout

    def init(self):
        self.cmd([self.bin/'initdb','-D',self.data,'--no-locale','--encoding=UTF8','-A','trust','-U','course_owner'])
        options = ("\nlisten_addresses=''\nunix_socket_directories='{}'\nunix_socket_permissions=0700\n"
                   "port={}\nautovacuum=off\nfsync=on\nsynchronous_commit=on\n"
                   "shared_buffers='32MB'\nlog_line_prefix='%m [%p] '\n").format(self.sock,self.port)
        with (self.data/'postgresql.conf').open('a') as f:f.write(options)
        self.start()

    def start(self):
        self.cmd([self.bin/'pg_ctl','-D',self.data,'-l',self.root/'server.log','-w','-t','30','start'])

    def stop(self,mode):
        self.cmd([self.bin/'pg_ctl','-D',self.data,'-m',mode,'-w','-t','30','stop'])

    def connect(self,name):
        c=Connection(self.pq,self.sock,self.port,name)
        self.clients.append(c)
        return c

    def close_clients(self):
        for c in self.clients:c.close()
        self.clients.clear()

    def close(self):
        self.close_clients()
        if (self.data/'postmaster.pid').exists():self.stop('fast')
        if (self.root/'server.log').exists():shutil.copy2(self.root/'server.log',self.out/'server.log')
        shutil.rmtree(self.root)


def statistics(c):
    result={}
    for table,b in [('corr','i%10'),('uniform_pairs','(i/10)%10')]:
        c.run(f'CREATE TABLE {table}(id int PRIMARY KEY,a int,b int)')
        c.run(f'INSERT INTO {table} SELECT i,i%10,{b} FROM generate_series(0,999) i')
        c.run(f'ANALYZE {table}')
        record={'marginals':c.run(f"SELECT attname,most_common_freqs FROM pg_stats WHERE tablename='{table}' AND attname IN ('a','b') ORDER BY 1")}
        for model in ('ordinary','mcv','dependencies'):
            if model!='ordinary':
                c.run(f'CREATE STATISTICS joint ({model}) ON a,b FROM {table}')
                c.run(f'ANALYZE {table}')
            for right in (2,3):
                plan=c.plan(f'SELECT id FROM {table} WHERE a=2 AND b={right}')
                actual=(100 if right==2 else 0) if table=='corr' else 10
                expected=(10 if model=='ordinary' or table=='uniform_pairs' else
                          1 if model=='mcv' and right==3 else 100)
                require((plan['Plan Rows'],plan['Actual Rows'])==(expected,actual),'Statistics model prediction did not match plan')
                record[f'{model}_b{right}']=plan
            if model!='ordinary':c.run('DROP STATISTICS joint')
        result[table]=record
    return result


def visibility(cluster,c):
    c.run('CREATE TABLE cover_rows(id int NOT NULL,label int,payload text) WITH (fillfactor=60)')
    c.run("INSERT INTO cover_rows SELECT i,i%20,repeat('x',160) FROM generate_series(1,3000) i")
    c.run('CREATE UNIQUE INDEX cover_idx ON cover_rows(id) INCLUDE(label)')
    c.run('VACUUM (ANALYZE) cover_rows')
    extension=c.scalar("SELECT count(*) FROM pg_available_extensions WHERE name='pg_visibility'")=='1'
    if extension:c.run('CREATE EXTENSION pg_visibility')
    def query(k):return f'SELECT id,label FROM cover_rows WHERE id={k}'
    stages={}
    def measure(stage,expected):
        rows={}
        for key,want in [(250,expected),(2250,0)]:
            plan=c.plan(query(key))
            fetched=plan['Heap Fetches']
            require(plan['Node Type']=='Index Only Scan' and plan['Index Name']=='cover_idx','Expected natural Index Only Scan')
            require(c.run(query(key))==[[str(key),str(key%20)]] and fetched==want,'Heap Fetches or projected result changed')
            block=int(c.scalar(f"SELECT split_part(trim(both '()' from ctid::text),',',1) FROM cover_rows WHERE id={key}"))
            visible=c.scalar(f"SELECT all_visible FROM pg_visibility_map('cover_rows',{block})") if extension else None
            if extension:require(visible==('f' if want else 't'),'Wrong visibility map bit')
            rows[key]={'heap_block':block,'all_visible':visible,'plan':plan}
        require(rows[250]['heap_block']!=rows[2250]['heap_block'],'Target and control must occupy different heap pages')
        stages[stage]=rows
    measure('clean',0)
    old=cluster.connect('old_snapshot')
    old.run('BEGIN ISOLATION LEVEL REPEATABLE READ')
    old.run(query(250))
    c.run("UPDATE cover_rows SET payload=repeat('y',160) WHERE id=250")
    measure('changed',1)
    c.run('VACUUM cover_rows')
    measure('old_snapshot_held',1)
    old.run('COMMIT')
    c.run('VACUUM (DISABLE_PAGE_SKIPPING) cover_rows')
    measure('old_snapshot_released',0)
    return {'pg_visibility_installed':extension,'stages':stages}


def concurrency(cluster,c):
    c.run('CREATE TABLE lockrows(id int PRIMARY KEY,v int); INSERT INTO lockrows VALUES(1,10),(2,20)')
    a,b=cluster.connect('writer_a'),cluster.connect('writer_b')
    read='SELECT v,xmin,xmax,ctid FROM lockrows WHERE id=1'
    snapshots={}
    for level in ['READ COMMITTED','REPEATABLE READ']:
        a.run('BEGIN ISOLATION LEVEL '+level)
        first=a.run(read)
        b.run('UPDATE lockrows SET v=v+1 WHERE id=1')
        second=a.run(read)
        a.run('COMMIT')
        current=c.run(read)
        require([r[0][0] for r in (first,second,current)]==(['10','11','11'] if level=='READ COMMITTED' else ['11','11','12']),
                'Unexpected snapshot values')
        snapshots[level]={'first':first,'second':second,'fresh':current}
    def change(db,row,delta):
        try:
            db.run(f'UPDATE lockrows SET v=v+{delta} WHERE id={row}')
            return '00000'
        except SqlError as e:return e.code
    def edge(waiter,holder):
        return c.scalar(f'SELECT {holder.pid}=ANY(pg_blocking_pids({waiter.pid}))')=='t'
    state={}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        c.run('UPDATE lockrows SET v=0')
        a.run("SET deadlock_timeout='2s'; BEGIN; UPDATE lockrows SET v=v+1 WHERE id=1")
        b.run("SET deadlock_timeout='9s'; BEGIN; UPDATE lockrows SET v=v+10 WHERE id=2")
        t1=pool.submit(change,a,2,1)
        until(lambda:edge(a,b),'A did not wait for B')
        t2=pool.submit(change,b,1,10)
        until(lambda:edge(a,b) and edge(b,a),'Wait cycle not observed')
        locks=c.run(f'SELECT pid,locktype,mode,granted,transactionid FROM pg_locks WHERE pid IN ({a.pid},{b.pid}) ORDER BY 1,2,3')
        require(t1.result(12)=='40P01','A should detect the controlled deadlock')
        a.run('ROLLBACK')
        require(t2.result(12)=='00000','B should survive')
        b.run('COMMIT')
        values=c.run('SELECT v FROM lockrows ORDER BY id')
        require(values==[['10'],['10']],'Wrong deadlock survivor values')
        state['opposite']={'a_pid':a.pid,'b_pid':b.pid,'locks_in_cycle':locks,'victim':'40P01','final':values}
        c.run('UPDATE lockrows SET v=0')
        a.run('BEGIN; UPDATE lockrows SET v=v+1 WHERE id=1')
        b.run('BEGIN')
        t2=pool.submit(change,b,1,10)
        until(lambda:edge(b,a),'Expected one-way wait')
        a.run('UPDATE lockrows SET v=v+1 WHERE id=2; COMMIT')
        require(t2.result(12)=='00000','Ordered waiter failed')
        b.run('UPDATE lockrows SET v=v+10 WHERE id=2; COMMIT')
        state['same_order']=c.run('SELECT v FROM lockrows ORDER BY id')
        require(state['same_order']==[['11'],['11']],'Wrong ordered final values')
    return {'snapshots':snapshots,'lock_order':state}


def recovery(cluster,c):
    c.run('CREATE TABLE outcomes(tx text,part int,PRIMARY KEY(tx,part))')
    c.run('CHECKPOINT')
    start=c.scalar('SELECT pg_current_wal_insert_lsn()')
    clients,xids={},{}
    for name in ['ack','abort','unknown','open']:
        d=cluster.connect(name);clients[name]=d
        d.run(f"BEGIN; INSERT INTO outcomes VALUES ('{name}',1),('{name}',2)")
        xids[name]=d.scalar('SELECT txid_current()')
    clients['ack'].run('COMMIT')
    clients['abort'].run('ROLLBACK')
    clients['unknown'].send_commit_without_read()
    until(lambda:c.scalar("SELECT count(*) FROM outcomes WHERE tx='unknown'")=='2','Observer did not see unknown transaction')
    clients['unknown'].close()
    end=c.scalar('SELECT pg_current_wal_insert_lsn()')
    c.scalar('SELECT pg_switch_wal()')
    dump=cluster.cmd([cluster.bin/'pg_waldump','-p',cluster.data/'pg_wal','-s',start,'-e',end])
    (cluster.out/'wal.txt').write_text(dump,encoding='utf-8')
    rows='SELECT tx,part FROM outcomes ORDER BY tx,part'
    observed=c.run(rows)
    require(observed==[['ack','1'],['ack','2'],['unknown','1'],['unknown','2']],'Unexpected pre-stop visibility')
    for name,xid in xids.items():
        matching=[s for s in dump.splitlines() if re.search(r'\btx:\s*'+xid+r'\b',s)]
        require(any('Heap' in s for s in matching),f'Missing heap WAL for {name}')
        require(any('desc: COMMIT' in s for s in matching)==(name in ('ack','unknown')),f'Wrong commit WAL for {name}')
    fingerprints=[]
    for _ in range(2):
        cluster.stop('immediate')
        cluster.close_clients()
        cluster.start()
        c=cluster.connect('recovery_observer')
        restored=c.run(rows)
        require(restored==observed,'Repeated recovery changed transaction set')
        fingerprints.append(hashlib.sha256(json.dumps(restored,separators=(',',':')).encode()).hexdigest())
    log=(cluster.root/'server.log').read_text(errors='replace')
    require('redo starts at' in log and 'redo done at' in log,'Missing WAL redo messages')
    return {'xids':xids,'wal_start':start,'wal_end':end,'rows_after_restart':observed,'fingerprints':fingerprints}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pg-config',default='pg_config')
    p.add_argument('--output',type=Path,required=True,help='new directory for the actual observations')
    args=p.parse_args()
    require(os.name=='posix' and os.geteuid()!=0,'Use a non-root POSIX account')
    args.output.mkdir(parents=True,exist_ok=False)
    cl=Cluster(args.pg_config,args.output)
    try:
        cl.init()
        admin=cl.connect('observer')
        stamp={'version':admin.scalar('SELECT version()'),'method':'owned SQL sessions; GDB disabled','topics':{}}
        quick=cl.cmd([cl.bin/'psql','-X','-h',cl.sock,'-p',str(cl.port),'-U','course_owner','-d','postgres',
                      '-f',Path(__file__).with_name('mechanisms.sql')])
        (cl.out/'single-session-plans.txt').write_text(quick,encoding='utf-8')
        for name,fn in [('statistics',lambda:statistics(admin)),('visibility',lambda:visibility(cl,admin)),
                        ('concurrency',lambda:concurrency(cl,admin)),('commit',lambda:recovery(cl,admin))]:
            result=fn()
            (cl.out/(name+'.json')).write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
            stamp['topics'][name]='PASS'
            print(name,'PASS',flush=True)
        (cl.out/'summary.json').write_text(json.dumps(stamp,ensure_ascii=False,indent=2),encoding='utf-8')
    finally:cl.close()


if __name__=='__main__':
    main()
