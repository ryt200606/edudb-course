"""Session-bound interchange, structure and CLI acceptance; isolated directories."""
from __future__ import annotations
import argparse,csv,hashlib,http.client,importlib.util,io,json,pathlib,subprocess,time,urllib.error,urllib.request

APP=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('ch11_api_support',APP/'tests/acceptance.py');support=importlib.util.module_from_spec(spec);spec.loader.exec_module(support)
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--exe',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--large',action='store_true');a=p.parse_args();a.exe=a.exe.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);results=[]
 def record(name,body):
  start=time.monotonic()
  try:value=body();result={'name':name,'status':'PASS','details':value}
  except Exception as e:result={'name':name,'status':'FAIL','error':repr(e)}
  result['seconds']=round(time.monotonic()-start,4);results.append(result);save();print(json.dumps({k:v for k,v in result.items() if k!='details'}),flush=True)
 def save():(out/'result.json').write_text(json.dumps({'schema':'edudb-ch11-api.v1','status':'PASS' if all(r['status']=='PASS' for r in results) else 'FAIL','binary_sha256':hashlib.sha256(a.exe.read_bytes()).hexdigest(),'large_payload_test_requested':a.large,'checks':results},ensure_ascii=False,indent=2),encoding='utf8')
 with support.Server(a.exe,out/'api database 中文',APP/'web',frames=8,limit=256*1024*1024) as db:
  sid=db.session();q=lambda sql:db.sql(sid,sql)
  def op(kind,request,ok=True):
   result=db.api('/sessions/'+sid+'/'+kind,'POST',request,timeout=180);support.require(result.get('ok',result.get('status')=='PASS') is ok,kind+': '+str(result));return result
  def csv_roundtrip():
   q('CREATE TABLE import_people(id INTEGER PRIMARY KEY,name VARCHAR(100),note VARCHAR(200),score REAL,active BOOLEAN)')
   content='\ufeffid,name,note,score,active\r\n9223372036854775807,张三,"a,b\r\nsaid ""hi""",86.5,true\r\n2,"\\N",\\N,\\N,false\r\n3,"",NULL,0,true\r\n'
   imported=op('import',{'format':'csv','table':'import_people','content':content,'header':True})
   rows=q('SELECT * FROM import_people ORDER BY id')['rows']
   support.require(rows==[['2','\\N',None,None,False],['3','','NULL',0.0,True],['9223372036854775807','张三','a,b\r\nsaid "hi"',86.5,True]],'CSV typed/NULL/int64/newline results: '+str(rows))
   exported=op('export',{'format':'csv','table':'import_people','header':True});decoded=list(csv.reader(io.StringIO(exported['content'],newline='')))
   support.require(decoded[0]==['id','name','note','score','active'],'CSV export header');support.require(len(decoded)==4,'CSV export rowcount')
   q('CREATE TABLE import_clone(id INTEGER PRIMARY KEY,name VARCHAR(100),note VARCHAR(200),score REAL,active BOOLEAN)')
   op('import',{'format':'csv','table':'import_clone','content':exported['content'],'header':True})
   support.require(q('SELECT * FROM import_clone ORDER BY id')['rows']==rows,'CSV typed roundtrip differs')
   return {'import':imported,'rows':rows,'csv_sha256':hashlib.sha256(exported['content'].encode()).hexdigest()}
  record('csv_UTF8_BOM_quotes_newlines_NULL_int64_typed_roundtrip',csv_roundtrip)
  def atomic_failures():
   before=q('SELECT * FROM import_people ORDER BY id')['rows'];failures=[]
   for content in ['id,name,note,score,active\n4,ok,x,1,true\n2,dup,x,2,true\n','id,name,note,score,active\n4,ok,x,1,true\n5,bad,x,wrong,true\n','id,name,note,score,active\n4,ok,x,1,true\n5,"unterminated','id,name,note,score,active\n4,too,few\n']:
    value=op('import',{'format':'csv','table':'import_people','content':content},False);failures.append(value);support.require(q('SELECT * FROM import_people ORDER BY id')['rows']==before,'Partial import became visible')
   return {'failures':failures,'unchanged_rows':before}
  record('import_failures_whole_transaction_atomic',atomic_failures)
  def headers():
   q('CREATE TABLE csv_header(a INTEGER,b VARCHAR(20))')
   op('import',{'format':'csv','table':'csv_header','content':'b,a\n中文,7\n','header':True})
   support.require(q('SELECT a,b FROM csv_header')['rows']==[['7','中文']],'Header mapping failed')
   op('import',{'format':'csv','table':'csv_header','content':'a,a\n1,2\n'},False)
   op('import',{'format':'csv','table':'csv_header','content':'a,missing\n1,2\n'},False)
   op('import',{'format':'csv','table':'csv_header','content':'8,noheader\n','header':False})
   support.require(q('SELECT COUNT(*) FROM csv_header')['rows']==[['2']],'Header failure inserted data')
   return q('SELECT a,b FROM csv_header ORDER BY a')['rows']
  record('CSV_header_mapping_duplicate_unknown_noheader',headers)
  def sql_atomic():
   response=op('import',{'format':'sql','content':"CREATE TABLE bad_import(id INTEGER PRIMARY KEY);INSERT INTO bad_import VALUES(1);INSERT INTO bad_import VALUES(1);"},False)
   support.require(db.sql(sid,'SELECT * FROM bad_import',ok=False)['error']['code']=='TABLE_NOT_FOUND','Failed SQL import kept CREATE')
   for script in ['BEGIN;SELECT 1;COMMIT;','SELECT 1;','CHECKPOINT;']:
    op('import',{'format':'sql','content':script},False)
   q('BEGIN');op('import',{'format':'csv','table':'csv_header','content':'a,b\n9,x\n'},False);q('ROLLBACK')
   return {'atomic_error':response,'transaction_commands_and_nonrestore_SQL_rejected':True}
  record('SQL_import_atomic_DDL_and_idle_only',sql_atomic)
  def blobs_dump():
   q('CREATE TABLE binary_data(id INTEGER PRIMARY KEY,data BLOB(100),name VARCHAR(40))')
   q("INSERT INTO binary_data VALUES(1,X'0001FF','中文'),(2,X'','empty'),(3,NULL,'missing')")
   csv_data=op('export',{'format':'csv','table':'binary_data'})
   q('CREATE TABLE binary_copy(id INTEGER PRIMARY KEY,data BLOB(100),name VARCHAR(40))')
   op('import',{'format':'csv','table':'binary_copy','content':csv_data['content']})
   rows=q('SELECT * FROM binary_data ORDER BY id')['rows'];support.require(q('SELECT * FROM binary_copy ORDER BY id')['rows']==rows,'BLOB CSV roundtrip')
   dump=op('export',{'format':'sql','table':'binary_data'})
   support.require("X'0001FF'" in dump['content'],'SQL export BLOB hex missing')
   other=db.session();db.close(other)
   with support.Server(a.exe,out/'logical restored 中文',APP/'web',frames=8,limit=256*1024*1024) as fresh:
    target=fresh.session();restored=fresh.api('/sessions/'+target+'/import','POST',{'format':'sql','content':dump['content']},timeout=180);support.require(restored.get('ok'),str(restored));support.require(fresh.sql(target,'SELECT * FROM binary_data ORDER BY id')['rows']==rows,'SQL logical dump roundtrip')
   return {'rows':rows,'dump_sha256':hashlib.sha256(dump['content'].encode()).hexdigest()}
  record('BLOB_hex_CSV_SQL_fresh_database_roundtrip',blobs_dump)
  def cli_transfer():
   source=out/'CLI 输入 中文.csv';source.write_bytes('id,name,note,score,active\r\n10,命令行,hello,91,true\r\n'.encode('utf8'))
   target=out/'CLI 导出 中文.csv';dump=out/'CLI 全库.sql';commands=[]
   for args in [['import','--format','csv','--table','import_people','--file',str(source)],['export','--format','csv','--table','import_people','--file',str(target)],['export','--format','sql','--file',str(dump)]]:
    r=subprocess.run([str(a.exe),*args,'--url',db.url],capture_output=True,timeout=180,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0));commands.append({'args':args,'exit':r.returncode,'stdout':r.stdout.decode('utf8'),'stderr':r.stderr.decode('utf8')});support.require(r.returncode==0,str(commands[-1]))
   before=target.read_bytes();refused=subprocess.run([str(a.exe),'export','--format','csv','--table','import_people','--file',str(target),'--url',db.url],capture_output=True,timeout=30,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
   support.require(refused.returncode!=0 and target.read_bytes()==before,'CLI overwrote existing file');return {'commands':commands,'existing_output_refused':True,'export_bytes':len(before),'dump_bytes':dump.stat().st_size}
  record('CLI_unicode_file_import_export_and_no_overwrite',cli_transfer)
  def inspect():
   result=op('inspect',{'table':'import_people','max_pages':256});support.require(result.get('table')=='import_people' or result.get('inspection',{}).get('table')=='import_people','Inspect table identity')
   for body in [{'table':'import_people','max_pages':0},{'table':'import_people','max_pages':2049},{'table':'does_not_exist'}]:op('inspect',body,False)
   return result
  record('inspect_readonly_structure_and_bounds',inspect)
  def request_limits():
   big='SELECT 1;--'+'x'*(1024*1024);response=db.api('/sessions/'+sid+'/sql','POST',{'sql':big});support.require((response.get('error')or{}).get('code')=='REQUEST_TOO_LARGE','SQL 1MiB bound changed: '+str(response)[:1000])
   for body in [[],{'format':True,'content':''},{'format':'bad','content':''},{'format':'csv','table':'csv_header','content':True}]:op('import',body,False)
   dedicated=op('import',{'format':'unsupported','content':'x'*(1024*1024)},False);support.require((dedicated.get('error')or{}).get('code')!='REQUEST_TOO_LARGE','Dedicated import incorrectly kept SQL 1MiB limit')
   result={'ordinary_SQL_limit':'PASS','malformed_requests':'PASS','64MiB_payload':'NOT_RUN'}
   if a.large:
    connection=http.client.HTTPConnection('127.0.0.1',db.port,timeout=10)
    try:
     connection.putrequest('POST','/api/v1/sessions/'+sid+'/import');connection.putheader('Content-Length',str(64*1024*1024+1));connection.putheader('Content-Type','application/json');connection.endheaders();response=connection.getresponse();too_big=json.loads(response.read());support.require(response.status==413 and (too_big.get('error')or{}).get('code')=='REQUEST_TOO_LARGE','Import 64MiB declared payload bound: '+str(too_big));result['64MiB_payload']='PASS_HEADER_REJECTED_BEFORE_BODY'
    finally:connection.close()
   return result
  record('request_types_old_SQL_bound_and_dedicated_import_limit',request_limits)
  db.close(sid)
 save();return 0 if all(r['status']=='PASS' for r in results) else 1
if __name__=='__main__':raise SystemExit(main())
