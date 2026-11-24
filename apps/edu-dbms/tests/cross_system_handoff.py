"""Exercise Windows -> WSL2 physical replay and controlled cross-domain switchover."""
import argparse
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def require(condition, text):
    if not condition:
        raise RuntimeError(text)


def execute(args, error=None, stdin=None):
    p = subprocess.run(list(map(str, args)), input=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       timeout=60, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if error:
        require(p.returncode != 0 and error in p.stderr.decode('utf-8', 'replace'),
                f'Expected {error}; stderr={p.stderr[-1600:]!r}')
    elif p.returncode:
        raise RuntimeError(f'Process failed ({p.returncode}): {p.stderr[-2000:]!r}')
    return p.stdout.decode('utf-8').strip()


def api(port, endpoint, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}' + endpoint, data=data,
                                 headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.load(resp)


def session(port):
    limit = time.monotonic() + 12
    while True:
        try:
            return api(port, '/api/v1/sessions', {})['session_id']
        except OSError:
            if time.monotonic() >= limit:
                raise
            time.sleep(.1)


def sql(port, sid, text, error=None):
    out = api(port, '/api/v1/sessions/' + sid + '/sql', {'sql':text})
    require((not error and out.get('ok')) or
            (error and not out.get('ok') and out['error']['code']==error), f'SQL: {text} -> {out}')
    return out


def ports(count):
    with_sockets = [socket.socket() for _ in range(count)]
    try:
        for s in with_sockets:
            s.bind(('127.0.0.1', 0))
        return [s.getsockname()[1] for s in with_sockets]
    finally:
        for s in with_sockets:
            s.close()


class Scenario:
    def __init__(self, args, scratch):
        self.args, self.scratch = args, scratch
        self.windows = Path(args.windows_exe).resolve()
        self.db = scratch / 'source'
        self.guest = '/tmp/edudb-cross-' + secrets.token_hex(12)
        self.prefix = ['wsl.exe','-d',args.distribution,'--exec']
        self.key = scratch/'operator.key'
        self.key.write_text(secrets.token_hex(32),encoding='ascii')
        self.win_port, self.linux_port, self.receive = ports(3)
        self.token = secrets.token_hex(16)
        self.primary, self.linux_pid = None, None

    def ws(self, *args, **kw):
        return execute(self.prefix + list(args), **kw)

    def linux(self, *args, **kw):
        return self.ws(self.args.linux_exe, *args, **kw)

    def mapped(self, path):
        return self.ws('wslpath','-u', Path(path).resolve().as_posix())

    def start_primary(self, directory=None, receiver_token=None):
        command=[str(self.windows),'serve','--data',str(directory or self.db),'--port',str(self.win_port),
                 '--web', str(self.args.web.resolve())]
        if receiver_token:
            command+=['--replica-port',str(self.receive),'--replication-token',receiver_token]
        self.primary = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        return session(self.win_port)

    def stop_primary(self):
        if self.primary and self.primary.poll() is None:
            self.primary.terminate()
            self.primary.wait(timeout=12)
        self.primary=None

    def start_replica(self, receive=True):
        argv=[self.args.linux_exe,'serve','--data',self.guest+'/standby','--web',self.mapped(self.args.web.resolve()),
              '--port',str(self.linux_port)]
        if receive:
            argv += ['--replica-port',str(self.receive),'--replication-token',self.token]
        spawn=("import subprocess,sys; log=open(sys.argv[1],'ab'); "
               "p=subprocess.Popen(sys.argv[2:],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True); print(p.pid)")
        self.linux_pid = int(self.ws('python3','-c',spawn,self.guest+'/server.log',*argv))
        return session(self.linux_port)

    def stop_replica(self):
        if self.linux_pid is None:
            return
        # Verify both program and unique owned data directory before signalling.
        stop=("import os,pathlib,signal,sys,time\n"
              "p=pathlib.Path('/proc')/sys.argv[1]\n"
              "if p.exists() and (p/'exe').exists():\n"
              " assert (p/'exe').resolve()==pathlib.Path(sys.argv[2]).resolve()\n"
              " assert os.fsencode(sys.argv[3]) in (p/'cmdline').read_bytes().split(b'\\0')\n"
              " os.kill(int(sys.argv[1]),signal.SIGTERM)\n"
              " for i in range(100):\n"
              "  if not (p/'exe').exists(): break\n"
              "  time.sleep(.05)\n"
              " assert not (p/'exe').exists()\n")
        self.ws('python3','-c',stop,str(self.linux_pid),self.args.linux_exe,self.guest+'/standby')
        self.linux_pid=None

    def transfer(self, mode, file, error=None, db='standby'):
        args=['handoff-'+mode, '--data',str(self.db) if mode=='surrender' else self.guest+'/'+db,
              '--key-file', str(self.key) if mode=='surrender' else self.guest+'/operator.key']
        if file:
            args += ['--source', str(file) if mode=='surrender' else self.mapped(file)]
        if mode=='surrender':
            raw=execute([self.windows]+args,error)
        else:
            raw=self.linux(*args,error=error)
        return json.loads(raw) if not error else None

    def blob(self, name, body):
        path=self.scratch/name
        path.write_text(json.dumps(body),encoding='utf-8')
        return path

    def verify_signature(self, message):
        unsigned=dict(message)
        actual=unsigned.pop('mac')
        canonical=json.dumps(unsigned,sort_keys=True,separators=(',',':')).encode('utf-8')
        expected=hmac.new(bytes.fromhex(self.key.read_text('ascii')),canonical,hashlib.sha256).hexdigest()
        require(hmac.compare_digest(expected,actual),'Certificate differs from the independent HMAC-SHA256 implementation')

    def restore_and_return(self, order, first_rows, first_ticket):
        # The original Windows directory remains retired. Only its host rejoins.
        def durable_image():
            files=[self.db/'control.json',self.db/'data.pages',*self.db.glob('wal-*.log')]
            return {f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in files}
        original_image=durable_image()
        rejoined=self.scratch/'windows-restored-from-linux'
        staged=self.scratch/'return-backup'
        token=secrets.token_hex(16)
        source=self.start_replica(receive=False)
        snapshot=sql(self.linux_port,source,"BACKUP DATABASE AS 'returnbase'")['recovery']
        self.ws('cp','-a',snapshot['directory'],self.mapped(staged))
        execute([self.windows,'restore','--source',staged,'--data',rejoined])
        reader=self.start_primary(rejoined,token)
        require(sql(self.win_port,reader,order)['rows']==first_rows,'Base restore differs from Linux owner')
        sql(self.win_port,reader,"INSERT INTO witness VALUES(110,'standby rejects')",'STANDBY_READ_ONLY')
        sql(self.linux_port,source,"INSERT INTO witness VALUES(101,'on Linux after backup'),(102,'second pending archive')")
        latest=sql(self.linux_port,source,order)['rows']
        require(len(latest)==len(first_rows)+2 and latest[:-2]==first_rows,'Linux WAL witness rows')
        archive=sql(self.linux_port,source,'ARCHIVE WAL')['recovery']
        local_dir=self.scratch/'return-archive'
        local_dir.mkdir()
        # WSL2 NAT exposes guest loopback to Windows. For the return leg,
        # transfer the sealed files via the mount and use the host's loopback ship client.
        marker_name=Path(archive['manifest_path']).name
        archive_root=archive['manifest_path'].rsplit('/',1)[0]
        self.ws('cp','--',archive['manifest_path'],archive_root+'/'+archive['file'],self.mapped(local_dir))
        marker=local_dir/marker_name
        for name in (marker_name,archive['file']):
            require(hashlib.sha256((local_dir/name).read_bytes()).hexdigest()==
                    self.ws('sha256sum',archive_root+'/'+name).split()[0],'Return archive transfer changed bytes')
        def receive_status():
            q=urllib.request.Request(f'http://127.0.0.1:{self.receive}/status',headers={'X-EduDB-Token':token})
            with urllib.request.urlopen(q,timeout=8) as response:
                j=json.load(response)
                return {k:j[k] for k in ('received_lsn','forced_lsn','replayed_lsn')}
        before=receive_status()
        metadata=marker.read_bytes(); wal=(local_dir/archive['file']).read_bytes()
        envelope=len(metadata).to_bytes(8,'little')+metadata+wal
        rejected=urllib.request.Request(f'http://127.0.0.1:{self.receive}/receive',data=envelope,
                     headers={'Content-Type':'application/octet-stream','X-EduDB-Token':self.token})
        try:
            with urllib.request.urlopen(rejected,timeout=8):
                raise AssertionError('Return receiver accepted previous receiver credential')
        except urllib.error.HTTPError as denial:
            require(denial.code==403,'Expected HTTP 403 for retired receiver credential')
        require(receive_status()==before and sql(self.win_port,reader,order)['rows']==first_rows,
                'Rejected credential changed standby rows or LSNs')
        sent=json.loads(execute([self.windows,'ship','--source',marker,'--port',self.receive,'--replication-token',token]))
        after=receive_status()
        require(sent['sent_lsn']==archive['end_lsn'] and set(after.values())=={archive['end_lsn']},'Return replay did not reach the sender')
        require(sql(self.win_port,reader,order)['rows']==latest,'Returned standby rows differ from Linux owner')
        self.stop_primary()
        self.stop_replica()
        def windows_handoff(mode, file=None, error=None):
            cmd=[self.windows,'handoff-'+mode,'--data',rejoined,'--key-file',self.key]
            if file:cmd+=['--source',file]
            raw=execute(cmd,error=error)
            return json.loads(raw) if not error else None
        windows_handoff('accept',first_ticket,'HANDOFF_TARGET')
        offer=windows_handoff('offer')
        self.verify_signature(offer)
        offerfile=self.blob('return-offer.json',offer)
        ticket=json.loads(self.linux('handoff-surrender','--data',self.guest+'/standby','--key-file',
                                     self.guest+'/operator.key','--source',self.mapped(offerfile)))
        self.verify_signature(ticket)
        ticketfile=self.blob('return-ticket.json',ticket)
        accepted=windows_handoff('accept',ticketfile)
        require(accepted['writable'] and accepted['timeline']==ticket['timeline'] and ticket['epoch']==3,'Return epoch/ownership')
        # Retrying a now-retired primary's earlier accept must never attest writability.
        self.transfer('accept',first_ticket,'FENCED')
        previous=self.start_replica(receive=False)
        sql(self.linux_port,previous,"INSERT INTO witness VALUES(115,'retired Linux')",'FENCED')
        self.stop_replica()
        old=self.start_primary()
        sql(self.win_port,old,"INSERT INTO witness VALUES(116,'retired Windows directory')",'FENCED')
        self.stop_primary()
        require(durable_image()==original_image,'Retired Windows durable data files were changed')
        new=self.start_primary(rejoined)
        require(sql(self.win_port,new,order)['rows']==latest,'Returned owner lost a committed row')
        sql(self.win_port,new,"INSERT INTO witness VALUES(103,'new writable Windows directory')")
        final=sql(self.win_port,new,order)['rows']
        require(len(final)==len(latest)+1 and final[:-1]==latest,'Reverse commit lost earlier rows')
        self.stop_primary()
        require(windows_handoff('accept',ticketfile)['idempotent'],'Reverse accept retry not idempotent')
        return {'base_rows':len(first_rows),'caught_up_rows':len(latest),'reverse_owner_rows':len(final),
                'read_only_until_transfer':True,'retired_receiver_credential_http':403,
                'previous_transfer_ticket_rejected':True,'old_directories_refused_writes':True,
                'retired_windows_files_unchanged':original_image,'return_backup_and_archive':'WSL shared mount with SHA-256 checked archive and Windows authenticated loopback replay',
                'before_receipt':before,'after_receipt':after,'sender_lsn':sent['sent_lsn'],'epoch':ticket['epoch'],
                'timeline':ticket['timeline'],'linux_and_joined_windows_fingerprint':hashlib.sha256(json.dumps(latest,separators=(',',':')).encode()).hexdigest(),
                'reverse_owner_final_fingerprint':hashlib.sha256(json.dumps(final,separators=(',',':')).encode()).hexdigest()}

    def run(self):
        guest=self.guest
        self.ws('mkdir','-m','700',guest)
        self.ws('install','-m','600',self.mapped(self.key),guest+'/operator.key')
        leak=self.ws('stat','-c','%a',guest+'/operator.key')
        require(leak=='600','WSL key is not private')
        sid=self.start_primary()
        sql(self.win_port,sid,'CREATE TABLE witness(id INT PRIMARY KEY, value VARCHAR(60))')
        sql(self.win_port,sid,"INSERT INTO witness VALUES(0,'seed')")
        base=sql(self.win_port,sid,"BACKUP DATABASE AS 'crossbase'")['recovery']
        copy=self.mapped(Path(base['directory']))
        self.ws('cp','-a',copy,guest+'/seed')
        for db in ['standby','other']:
            self.linux('restore','--source',guest+'/seed','--data',guest+'/'+db)
        stale=self.blob('stale.json',self.transfer('offer',None))
        rep=self.start_replica()
        sql(self.linux_port,rep,'PROMOTE STANDBY','FENCE_MISSING')
        sql(self.linux_port,rep,"INSERT INTO witness VALUES(66,'reject')",'STANDBY_READ_ONLY')
        order='SELECT id,value FROM witness ORDER BY id'
        require(len(sql(self.linux_port,rep,order)['rows'])==1,'Standby baseline incorrect')
        self.stop_replica()
        sql(self.win_port,sid,'INSERT INTO witness VALUES '+','.join("({},'value-{}')".format(n,n) for n in range(1,65)))
        rows=sql(self.win_port,sid,order)['rows']
        require(len(rows)==65,'Primary row count')
        archive=sql(self.win_port,sid,'ARCHIVE WAL')['recovery']
        ship=[self.windows,'ship','--source',archive['manifest_path'],'--port',str(self.receive),'--replication-token',self.token]
        execute(ship,'REPLICATION_OUTCOME_UNKNOWN')
        self.stop_primary()
        self.transfer('surrender',stale,'HANDOFF_NOT_CAUGHT_UP')
        sid=self.start_primary()
        require(sql(self.win_port,sid,order)['rows']==rows,'Stale challenge changed source data')
        rep=self.start_replica()
        start=time.monotonic()
        ack=json.loads(execute(ship))
        seconds=time.monotonic()-start
        duplicate=json.loads(execute(ship))
        require(ack['replayed_lsn']==duplicate['replayed_lsn']==archive['end_lsn'],'Replay did not catch up')
        require(sql(self.linux_port,rep,order)['rows']==rows,'Cross-system replay mismatch')
        self.stop_replica()
        self.stop_primary()
        offer=self.transfer('offer',None)
        self.verify_signature(offer)
        final_offer=self.blob('offer.json',offer)
        self.transfer('accept',final_offer,'HANDOFF_PROOF')
        altered=json.loads(final_offer.read_text());altered['fork_lsn']+=1
        self.transfer('surrender',self.blob('tampered.json',altered),'HANDOFF_PROOF')
        begin=time.monotonic()
        ticket=self.transfer('surrender',final_offer)
        self.verify_signature(ticket)
        signed=self.blob('ticket.json',ticket)
        first=self.transfer('accept',signed)
        require(first['writable'] and first['timeline']==ticket['timeline'],'New owner not writable')
        promoted=self.start_replica(receive=False)
        require(sql(self.linux_port,promoted,order)['rows']==rows,'New primary did not serve correct history')
        elapsed=time.monotonic()-begin
        self.stop_replica()
        require(ticket==self.transfer('surrender',final_offer),'Repeated surrender did not re-issue identical ticket')
        self.transfer('accept',signed,'HANDOFF_TARGET',db='other')
        require(self.transfer('accept',signed)['idempotent'],'Accept retry was not idempotent')
        old=self.start_primary()
        sql(self.win_port,old,"INSERT INTO witness VALUES(90,'old primary')",'FENCED')
        require(sql(self.win_port,old,order)['rows']==rows,'Fenced old primary changed data')
        promoted=self.start_replica(receive=False)
        sql(self.linux_port,promoted,"INSERT INTO witness VALUES(100,'new primary')")
        final=sql(self.linux_port,promoted,order)['rows']
        require(len(final)==66 and final[:-1]==rows,'Promoted primary lost history')
        self.stop_replica()
        require(self.transfer('accept',signed)['idempotent'],'Old ticket failed after new acknowledged commits')
        self.stop_primary()
        return_leg=self.restore_and_return(order,final,signed)
        wal_bytes=Path(archive['manifest_path']).with_name(archive['file']).stat().st_size
        return {'status':'PASS','primary':'Windows native','standby':self.ws('uname','-sr'),'transport':'WSL2 loopback forwarding',
                'windows_sha256':hashlib.sha256(self.windows.read_bytes()).hexdigest(),
                'linux_sha256':self.ws('sha256sum',self.args.linux_exe).split()[0],
                'base_lsn':base['end_lsn'] if 'end_lsn' in base else json.loads(stale.read_text())['fork_lsn'],
                'archive_end_lsn':archive['end_lsn'],'forced_lsn':ack['forced_lsn'],'replayed_lsn':ack['replayed_lsn'],
                'sender_archive_bytes':wal_bytes,'observed_catchup_bytes_per_second':wal_bytes/seconds,
                'elapsed_explicit_ship_seconds_including_cli':seconds,'surrender_start_to_first_new_primary_query_seconds':elapsed,
                'measurement_scope':'one end-to-end functional run; CLI and WSL process launch included; no physical hardware failover',
                'replayed_rows':len(rows),'pre_handoff_fingerprint':hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest(),
                'new_primary_rows':len(final),'target_epoch':ticket['epoch'],'target_timeline':ticket['timeline'],'rejoin_and_reverse':return_leg,
                'checks':['disconnected shipment outcome unknown','lagging offer refused','standby refused SQL writes',
                          'independent domain ordinary PROMOTE refused','tampered offer refused','wrong candidate refused',
                          'idempotent WAL shipment','repeated surrender returned identical ticket',
                          'idempotent accept before and after new writes','old primary restart refused writes','new primary preserved rows']}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--windows-exe',required=True)
    parser.add_argument('--linux-exe',required=True)
    parser.add_argument('--distribution',default='Ubuntu-24.04')
    parser.add_argument('--web',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='edudb-handoff-') as scratch:
        scenario=Scenario(args,Path(scratch))
        try:
            result=scenario.run()
        finally:
            scenario.stop_replica()
            scenario.stop_primary()
            scenario.ws('python3','-c',
                "import pathlib,re,shutil,sys;p=pathlib.Path(sys.argv[1]).resolve();"
                "assert p.parent==pathlib.Path('/tmp') and re.fullmatch('edudb-cross-[0-9a-f]{24}',p.name);"
                "shutil.rmtree(p) if p.exists() else None",scenario.guest)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))
