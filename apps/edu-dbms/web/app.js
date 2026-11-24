'use strict';
const $=id=>document.getElementById(id),sessions=new Map();
const storageKey='edudb-owned-sessions-v2',ownerKey='edudb-tab-owner-v2',pageSize=100;
let current=null,sessionNumber=0,connecting=false,leaving=false,diagnosticRequest=0;
let pendingCleanup=new Set(),releaseOwnership=null,ownershipNotice='';
try{pendingCleanup=new Set(JSON.parse(sessionStorage.getItem(storageKey)||'[]').filter(x=>typeof x==='string'));}catch{sessionStorage.removeItem(storageKey);}
function rememberOwned(){sessionStorage.setItem(storageKey,JSON.stringify([...new Set([...pendingCleanup,...sessions.keys()])]));}
async function claimOwnership(){
  const token=sessionStorage.getItem(ownerKey)||crypto.randomUUID();sessionStorage.setItem(ownerKey,token);
  // A duplicated tab inherits sessionStorage. A live document holding this lock
  // proves the inherited session IDs belong to it, so the new tab must fork.
  let acquired=null;
  if(navigator.locks?.request)acquired=await new Promise(resolve=>{
    navigator.locks.request('edudb-tab-'+token,{ifAvailable:true},lock=>{
      if(!lock){resolve(false);return;}
      return new Promise(release=>{releaseOwnership=release;resolve(true);});
    }).catch(()=>resolve(null));
  });
  if(acquired===false){pendingCleanup.clear();sessionStorage.setItem(ownerKey,crypto.randomUUID());rememberOwned();return claimOwnership();}
  if(acquired===null){pendingCleanup.clear();ownershipNotice='本浏览器无法确认旧页会话归属，旧页未完成事务由原页显式关闭或服务端闲置超时回滚。';}
  rememberOwned();
}
function message(text,error=false){$('message').textContent=text;$('message').classList.toggle('error',error);}
function connection(online,text){$('status').textContent=text||(online?'127.0.0.1 · 已连接':'连接中断');$('lamp').classList.toggle('online',online);}
function sqlError(error){return `${error.code||'ERROR'}：${error.message||'执行失败'}${Number.isInteger(error.position)&&error.position>=0?'（字节位置 '+error.position+'）':''}`;}
// Preserve integer diagnostics (for example B+tree keys/LSNs) beyond 2^53.
// Scan only JSON number tokens outside strings; quoted SQL values are unchanged.
function parseApiJson(text){
  let safe='',quoted=false,escaped=false;
  for(let i=0;i<text.length;i++){
    const c=text[i];
    if(quoted){safe+=c;if(escaped)escaped=false;else if(c==='\\')escaped=true;else if(c==='"')quoted=false;continue;}
    if(c==='"'){quoted=true;safe+=c;continue;}
    if(c==='-'||(c>='0'&&c<='9')){
      const token=text.slice(i).match(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/)?.[0];
      if(token){safe+=!/[.eE]/.test(token)&&!Number.isSafeInteger(Number(token))?JSON.stringify(token):token;i+=token.length-1;continue;}
    }
    safe+=c;
  }
  return JSON.parse(safe);
}
async function api(path,method='GET',body,timeout=10000){
  const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),timeout);
  try{
    const r=await fetch('/api/v1'+path,{method,headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined,signal:abort.signal});
    let j;try{j=parseApiJson(await r.text());}catch(original){if(abort.signal.aborted||original.name==='AbortError')throw original;const e=new Error('服务未返回有效 JSON');e.transport=true;throw e;}
    if(!r.ok){const e=new Error(j.error?.message||r.statusText);e.code=j.error?.code;e.position=j.error?.position;throw e;}return j;
  }catch(e){if(abort.signal.aborted||e.name==='AbortError'){const timeoutError=new Error('请求超过等待上限');timeoutError.transport=true;throw timeoutError;}if(!e.code)e.transport=true;throw e;}finally{clearTimeout(timer);}
}
function updateControls(){
  const s=sessions.get(current),disabled=!s||s.busy||s.closing||s.invalid||connecting;
  $('execute').disabled=disabled;document.querySelectorAll('[data-sql]').forEach(b=>b.disabled=disabled);
  document.querySelectorAll('[data-session-action]').forEach(b=>b.disabled=disabled);
  $('new-session').disabled=connecting;$('reconnect').disabled=connecting||[...sessions.values()].some(s=>s.busy||s.closing);
}
function renderSessions(){
  $('sessions').replaceChildren();
  for(const [id,s]of sessions){
    const row=document.createElement('div');row.className='session-row';
    const b=document.createElement('button');b.className='session-item'+(id===current?' active':'');b.textContent=s.name+(s.closing?' · 关闭中':s.busy?' · 执行中':'');b.onclick=()=>selectSession(id);
    const close=document.createElement('button');close.className='session-close';close.textContent='×';close.title='关闭并回滚未完成事务';close.setAttribute('aria-label','关闭'+s.name+'并回滚');close.disabled=s.busy||s.closing||connecting;close.onclick=()=>closeSession(id);
    row.append(b,close);$('sessions').append(row);
  }
  const s=sessions.get(current);$('session-label').textContent=s?.name||'无会话';$('tx-state').textContent=s?.state||'idle';updateControls();refreshHelpRuntime();
}
function selectSession(id){
  if(!sessions.has(id))return;if(sessions.has(current))sessions.get(current).sql=$('sql').value;
  current=id;$('sql').value=sessions.get(id).sql;renderSessions();displayCurrent();
}
function emptyResults(text){
  $('result-meta').textContent='';$('result-table').replaceChildren();const p=document.createElement('p');p.className='empty';p.textContent=text;$('result-table').append(p);
  $('result-pagination').replaceChildren();$('plan-tree').replaceChildren();$('plan-json').textContent='尚无执行计划。';
}
function renderRows(j,s){
  const total=j.rows?.length||0,last=Math.max(0,Math.ceil(total/pageSize)-1);s.page=Math.min(s.page||0,last);const start=s.page*pageSize,end=Math.min(total,start+pageSize);
  $('result-meta').textContent=`${total} 行结果 · 影响 ${j.affected_rows||0} 行${total?' · 当前显示 '+(start+1)+'–'+end+' 行':''}`;
  const wrap=$('result-table');wrap.replaceChildren();$('result-pagination').replaceChildren();
  if(!j.columns?.length){const p=document.createElement('p');p.className='empty';p.textContent=j.ok?'语句执行完成。':'本次语句未返回结果。';wrap.append(p);return;}
  const table=document.createElement('table'),head=document.createElement('thead'),tr=document.createElement('tr');
  for(const col of j.columns){const th=document.createElement('th');th.textContent=typeof col==='string'?col:col.name;tr.append(th);}head.append(tr);table.append(head);
  const tbody=document.createElement('tbody');
  for(const row of (j.rows||[]).slice(start,end)){const line=document.createElement('tr');for(const v of row){const td=document.createElement('td');td.textContent=v===null?'NULL':typeof v==='object'?JSON.stringify(v):String(v);if(v===null)td.className='null';line.append(td);}tbody.append(line);}table.append(tbody);wrap.append(table);
  if(j.constraints?.length){
    const section=document.createElement('section'),heading=document.createElement('h3'),list=document.createElement('dl');
    section.className='table-constraints';section.setAttribute('aria-label','数据完整性约束');
    heading.textContent='数据完整性约束';
    const identifier=value=>'"'+String(value).replace(/"/g,'""')+'"';
    for(const rule of j.constraints){
      const name=document.createElement('dt'),definition=document.createElement('dd'),sql=document.createElement('code');
      name.textContent=rule.name;
      sql.textContent=rule.kind==='check'?`CHECK (${rule.sql})`:
        `FOREIGN KEY (${rule.columns.map(identifier).join(', ')}) REFERENCES ${identifier(rule.parent)} (${rule.keys.map(identifier).join(', ')}) ON DELETE ${rule.on_delete}`;
      definition.append(sql);list.append(name,definition);
    }
    section.append(heading,list);wrap.append(section);
  }
  if(total>pageSize){
    const prev=document.createElement('button'),next=document.createElement('button'),label=document.createElement('span');prev.textContent='上一页';prev.id='result-prev';prev.disabled=s.page===0;next.textContent='下一页';next.id='result-next';next.disabled=s.page===last;
    label.textContent=`第 ${s.page+1} / ${last+1} 页 · 每页 ${pageSize} 行（仅显示分页）`;
    prev.onclick=()=>{s.page--;renderRows(j,s);};next.onclick=()=>{s.page++;renderRows(j,s);};$('result-pagination').append(prev,label,next);
  }
}
function displayCurrent(){
  const s=sessions.get(current);if(!s){emptyResults('连接后创建会话即可执行 SQL。');return;}
  if(s.result){renderRows(s.result,s);$('plan-tree').replaceChildren(EduDBPlan.render(s.result.plan,s.result.metrics));$('plan-json').textContent=JSON.stringify({plan:s.result.plan||null,metrics:s.result.metrics||{}},null,2);}
  else emptyResults(s.busy?'本会话正在执行，请等待。':'本会话尚无查询结果。');message(s.message||'本会话就绪。',s.error||false);
}
async function cleanPending(){for(const id of [...pendingCleanup]){await api('/sessions/'+encodeURIComponent(id),'DELETE');pendingCleanup.delete(id);rememberOwned();}}
async function createSession(){
  if(connecting)return;connecting=true;updateControls();
  try{
    await cleanPending();const j=await api('/sessions','POST',{});if(typeof j.session_id!=='string')throw new Error('服务缺少会话编号');
    const id=j.session_id;sessions.set(id,{name:'会话 '+(++sessionNumber),state:'idle',sql:$('sql').value,busy:false,result:null,page:0,message:ownershipNotice||'本会话就绪。'});rememberOwned();selectSession(id);connection(true);
  }catch(e){message('建立会话失败：'+e.message+'。请点击重新连接；遗留会话将在连接恢复后清理。',true);connection(false,'连接失败');}
  finally{connecting=false;renderSessions();}
}
async function closeSession(id){
  const s=sessions.get(id);if(!s||s.busy||s.closing)return;s.closing=true;s.message='正在关闭'+s.name+'并等待服务端回滚。';s.error=false;renderSessions();if(current===id)displayCurrent();
  try{
    await api('/sessions/'+encodeURIComponent(id),'DELETE');sessions.delete(id);pendingCleanup.delete(id);rememberOwned();if(current===id)current=null;const next=current||sessions.keys().next().value;
    if(next)selectSession(next);else{renderSessions();emptyResults('会话已关闭，未完成事务已回滚。');await createSession();}await refreshDiagnostics();
  }catch(e){s.message='关闭'+s.name+'请求失败：'+e.message+(e.transport?'。本页保留入口，服务端关闭结果未知；请恢复连接后重新连接并核对。':'。请核对错误后重试。');s.error=true;if(e.transport){s.invalid=true;s.state='关闭结果未知';}connection(false);}
  finally{s.closing=false;renderSessions();if(current===id)displayCurrent();}
}
async function reconnect(){
  if(connecting||[...sessions.values()].some(s=>s.busy||s.closing))return;if(sessions.has(current))sessions.get(current).sql=$('sql').value;
  for(const id of sessions.keys())pendingCleanup.add(id);sessions.clear();current=null;rememberOwned();renderSessions();emptyResults('正在恢复连接并清理本页遗留会话。');await createSession();await refreshDiagnostics();
}
async function execute(override){
  const id=current,s=sessions.get(id);if(!s||s.busy||s.closing||s.invalid||connecting)return;let text=override;
  if(typeof text!=='string'){const editor=$('sql');text=editor.value.substring(editor.selectionStart,editor.selectionEnd)||editor.value;}if(!text.trim())return;
  s.sql=$('sql').value;s.busy=true;s.result=null;s.page=0;s.message='正在执行；其他会话可继续操作，锁等待由服务端限时处理。';s.error=false;renderSessions();displayCurrent();
  try{
    const j=await api('/sessions/'+encodeURIComponent(id)+'/sql','POST',{sql:text},30000);window.EduDBTraceCollector?.add(j.metrics?.io);s.state=j.transaction_state||'idle';s.transactionOptions=s.state==='idle'?null:(j.transaction_options||s.transactionOptions||null);refreshHelpRuntime();s.result=j;s.error=!j.ok;s.message=j.ok?'执行完成。'+(s.state==='active'?' 当前事务尚未提交。':''):sqlError(j.error||{});
    if(j.error?.code==='SESSION_NOT_FOUND'){s.invalid=true;s.message+='。会话已失效，请重新连接；写入不会自动重试。';}connection(true);
  }catch(e){
    s.error=true;s.invalid=!!e.transport||e.code==='SESSION_NOT_FOUND';s.state=e.transport?'结果未知':s.state;
    s.message=e.transport?e.message+'。连接中断的写入结果可能未知，请先核对实际数据，勿自动重试。重新连接将关闭本页旧会话并新建会话。':sqlError(e);
    if(e.code==='SESSION_NOT_FOUND')s.message+='。会话已失效，请重新连接；写入不会自动重试。';if(e.transport)connection(false);
  }finally{s.busy=false;renderSessions();if(current===id)displayCurrent();}await refreshDiagnostics();
}
function prettySection(name,value,open=false){const d=document.createElement('details');d.open=open;const s=document.createElement('summary');s.textContent=name;const p=document.createElement('pre');p.textContent=JSON.stringify(value,null,2);d.append(s,p);return d;}
async function refreshDiagnostics(){
  const request=++diagnosticRequest;
  try{
    const d=await api('/diagnostics');if(request!==diagnosticRequest||leaving)return;refreshHelpRuntime(d);const tables=d.tables||[];$('tables').replaceChildren();
    if(Array.isArray(tables))for(const t of tables){const name=typeof t==='string'?t:t.name,b=document.createElement('button');b.className='table-item';b.textContent=name;b.onclick=()=>{$('sql').value='DESCRIBE "'+name.replace(/"/g,'""')+'";';execute();};$('tables').append(b);}
    const area=$('kernel-sections');area.replaceChildren(EduDBWaitGraph.render(d.waits,d.locks));if(d.storage)area.append(EduDBDiagnostics.render(d.storage));
    for(const [name,key]of [['会话与事务','sessions'],['逻辑锁','locks'],['锁等待关系','waits'],['存储、缓冲与 WAL','storage'],['表与索引结构','tables']])if(d[key]!==undefined)area.append(prettySection(name,d[key],false));
    $('kernel-summary').textContent=`会话 ${Array.isArray(d.sessions)?d.sessions.length:sessions.size}　表 ${tables.length??Object.keys(tables).length}`;$('diagnostic-message').textContent='';
  }catch(e){if(request===diagnosticRequest){$('diagnostic-message').textContent='读取诊断快照失败：'+e.message+'。显示内容可能已过时，请恢复连接后刷新。';connection(false);}}
}
// Merged into the existing app.js; no extra static assets or API routes.
// Release owners enable pending lessons only after the matching unified gate.
const helpRelease={tree:true,logicalUndo:true,selfJoin:true,sharedScan:true};
let helpDiagnostics=null,helpCategory='basic';
const helpStep=(label,sql,observe,extra={})=>({label,sql,observe,...extra});
const beginStep=(role,protocol,mode='READ WRITE',extra={})=>helpStep(role+' · 开始新事务','BEGIN PROTOCOL '+protocol+' '+mode+';','在对应会话执行。先结束该会话已有事务；同时活动的会话使用同一协议。',{idle:true,protocol:protocol.split(' ')[0],...extra});
const helpGuides=[
 {id:'basic',chapter:'基础',title:'建表、中文、NULL与64位整数',intro:'先在独立演示库准备数据。所有按钮只填入一条SQL，由你点击“执行 SQL”。重复插入会触发主键约束。',steps:[
 helpStep('建立学生表','CREATE TABLE demo_students(id INTEGER PRIMARY KEY,name VARCHAR(40) NOT NULL,dept VARCHAR(30),score REAL,active BOOLEAN);','INTEGER是有符号64位；VARCHAR保存UTF-8；NULL与空文本不同。',{write:true}),
 helpStep('插入含中文和NULL的数据',"INSERT INTO demo_students VALUES(1,'张三','计算机',91.5,TRUE),(2,'李四','计算机',78.0,FALSE),(3,'王五',NULL,NULL,TRUE),(9007199254740993,'大整数同学','自动化',88.0,TRUE);",'最后一个ID超过JavaScript安全整数范围，结果区应保持十进制原值。',{write:true}),
 helpStep('读取与NULL判断','SELECT id,name,dept,score FROM demo_students WHERE dept IS NULL OR score>=85 ORDER BY id;','用IS NULL判断空值；普通比较NULL产生UNKNOWN，WHERE保留TRUE。'),
 helpStep('增列并填充旧行','ALTER TABLE demo_students ADD COLUMN note VARCHAR(40) DEFAULT \'课程演示\';','DDL在事务内重写；检查旧记录的新列常量填充值。',{write:true}),
 helpStep('核对结构','DESCRIBE demo_students;','查看列类型、默认值和物理组织。结果来自内核。') ]},
 {id:'crud',chapter:'基础',title:'更新、删除与完整回滚',intro:'此例使用默认STRICT_2PL。逐条运行，观察事务内改变，最后回滚恢复。',steps:[beginStep('当前会话','STRICT_2PL'),
 helpStep('修改成绩','UPDATE demo_students SET score=score+5 WHERE id=1;','修改处于当前事务中，尚未对其他事务提交。',{write:true}),
 helpStep('删除一行','DELETE FROM demo_students WHERE id=2;','事务内查询能看到删除后的状态。',{write:true}),
 helpStep('观察事务内结果','SELECT id,name,score FROM demo_students ORDER BY id;','确认改动后再回滚。'),helpStep('回滚','ROLLBACK;','丢弃本事务修改并释放锁。'),
 helpStep('观察恢复后的结果','SELECT id,name,score FROM demo_students ORDER BY id;','张三恢复原成绩，李四重新出现。') ]},
 {id:'records',chapter:'10',title:'真正的定长与变长记录',intro:'RECORD FORMAT FIXED/VARIABLE是二进制记录布局，当前只用于HEAP。非HEAP组合会明确拒绝。',steps:[
 helpStep('建立定长布局','CREATE TABLE demo_fixed(id INTEGER PRIMARY KEY,enabled BOOLEAN,label VARCHAR(40)) RECORD FORMAT FIXED;','定长字段、NULL位图与显式字符串容量参与布局。',{write:true}),
 helpStep('插入定长记录',"INSERT INTO demo_fixed VALUES(1,TRUE,'中文定长记录'),(2,NULL,NULL);",'NULL不等于空串或FALSE。',{write:true}),
 helpStep('读取真实布局','DESCRIBE demo_fixed;','查看record_layout中的定长字节数、NULL位图和边界。'),
 helpStep('建立变长布局','CREATE TABLE demo_variable(id INTEGER PRIMARY KEY,label VARCHAR(200)) RECORD FORMAT VARIABLE;','变长目录记录字段偏移与长度。',{write:true}),helpStep('核验存储','VERIFY;','完整性检查包含记录、稳定行号、组织与辅助索引。') ]},
 {id:'buffer',chapter:'10',title:'缓冲pin、替换和顺序预读',intro:'REDO_PRIVATE提供LRU/MRU/CLOCK/TOSS_IMMEDIATE与同步read-ahead；ARIES_PAGE由自己的LRU authority buffer管理。访问计数用于解释机制。',steps:[
 helpStep('查看真实缓冲状态','SHOW BUFFER;','关注命中/未命中、pin、淘汰与预读计数；操作系统缓存仍会影响实际文件调用。'),
 helpStep('使用CLOCK','SET BUFFER POLICY CLOCK;','实例运行时配置，需要idle；此设置不切换恢复模式。',{idle:true,modes:['REDO_PRIVATE']}),
 helpStep('顺序预读两页','SET BUFFER READ_AHEAD 2;','同步预取可能减少后续demand miss，也可能有未使用预取。',{idle:true,modes:['REDO_PRIVATE']}),
 helpStep('关闭预读','SET BUFFER READ_AHEAD 0;','回到按需读取，便于机制对照。',{idle:true,modes:['REDO_PRIVATE']}),helpStep('查看受管介质','SHOW MEDIA;','软件RAID布局、缺失成员与校验诊断；多文件软件冗余不代表实机独立磁盘测试。') ]},
 {id:'cluster',chapter:'10',title:'多表共享聚簇页',intro:'聚簇成员实际共享堆页，以table OID区分记录，并保留各自的行号映射。成员加入/脱离是显式DDL。',steps:[
 helpStep('建立聚簇','CREATE CLUSTER demo_group;','先创建共享物理组织。',{write:true}),
 helpStep('加入第一张表','CREATE TABLE demo_people(id INTEGER PRIMARY KEY,dept INTEGER,name VARCHAR(30)) CLUSTER demo_group(dept);','聚簇键类型须与其他成员兼容，BLOB不能作聚簇键。',{write:true}),
 helpStep('加入第二张表','CREATE TABLE demo_room(id INTEGER PRIMARY KEY,dept INTEGER,room VARCHAR(30)) CLUSTER demo_group(dept);','这是另一张表，不是单表ORDERED的别名。',{write:true}),
 helpStep('写入成员',"INSERT INTO demo_people VALUES(1,10,'同部门学生');",'共享cluster资源保护成员的物理一致性。',{write:true}),
 helpStep('读取聚簇页','INSPECT CLUSTER demo_group LIMIT 256;','页中应有成员归属、共享组及实际行号信息。') ]},
 {id:'bplus',chapter:'11',title:'B+树分裂、访问路径与真实结构',intro:'小max_keys用于教学触发分裂；实际运行仍受页字节预算约束。先完成建表，再到结构视图读取。',inspect:{table:'demo_idx',index:'demo_idx_k'},steps:[
 helpStep('建立测试表','CREATE TABLE demo_idx(id INTEGER PRIMARY KEY,k INTEGER);','使用独立demo_表。',{write:true}),
 helpStep('建立小节点B+索引','CREATE INDEX demo_idx_k ON demo_idx USING BPLUS(k) WITH(max_keys=5);','max_keys是键项上限，内部子指针上限还要加1。',{write:true}),
 helpStep('插入24个键','INSERT INTO demo_idx VALUES '+Array.from({length:24},(_,i)=>'('+(i+1)+','+((i+1)*10)+')').join(',')+';','SQL增量插入会触发真实节点分裂和根变化。',{write:true}),
 helpStep('范围访问与统计','EXPLAIN ANALYZE SELECT id,k FROM demo_idx USE INDEX(demo_idx_k) WHERE k>=60 AND k<=160 ORDER BY k LIMIT 5;','LIMIT的返回行数不等于扫描行数或页数；查看排序/TopK、实际访问与可能的提前停止。'),
 helpStep('删除一段键','DELETE FROM demo_idx WHERE k>=80 AND k<=180;','检查结构计数，按实际数据观察借位、合并或根收缩。',{write:true}) ]},
 {id:'sparse',chapter:'11',title:'稀疏目录与物理顺序组织',intro:'稀疏索引省略部分索引项，表记录仍完整。平目录与多级目录应使用各自相匹配的ORDERED组织。',inspect:{table:'demo_ordered',index:'demo_sparse'},steps:[
 helpStep('建立平目录有序文件','CREATE TABLE demo_ordered(id INTEGER PRIMARY KEY,k INTEGER) ORGANIZATION ORDERED(k) WITH(multilevel=FALSE,max_keys=5);','物理记录按组织搜索码排序。',{write:true}),
 helpStep('建立稀疏别名','CREATE INDEX demo_sparse ON demo_ordered USING SPARSE(k);','目录模式与物理组织必须一致；不允许在同一文件上假建相反目录。',{write:true}),
 helpStep('多级组织模板','CREATE TABLE demo_multi(id INTEGER PRIMARY KEY,k INTEGER) ORGANIZATION ORDERED(k) WITH(multilevel=TRUE,max_keys=5);','另一张文件用于多级对照。',{write:true}),
 helpStep('建立多级目录','CREATE INDEX demo_multilevel ON demo_multi USING MULTILEVEL(k);','把相同数据分别插入两表后再比较目录。',{write:true}) ]},
 {id:'hash-bitmap',chapter:'11',title:'散列、位图、覆盖与多码访问',intro:'散列适合等值定位，有序索引支持范围；位图按SQL三值逻辑处理NULL。复合码、索引交集和覆盖访问是否实选，由计划返回。',steps:[
 helpStep('建立可扩展散列','CREATE INDEX demo_hash ON demo_idx USING HASH_EXTENDIBLE(k) WITH(max_keys=5,max_depth=8);','读取目录深度、桶分裂与溢出；线性散列使用另一种逐桶增长机制。',{write:true}),
 helpStep('强制等值散列访问','EXPLAIN ANALYZE SELECT id,k FROM demo_idx USE INDEX(demo_hash) WHERE k=60;','实际访问路径应明确为该索引；不把散列等值优势泛化到范围。'),
 helpStep('建立混合位图','CREATE INDEX demo_active ON demo_students USING BITMAP_HYBRID(active);','混合表示根据实际字节代价选择，存在位图区分NULL。',{write:true}),
 helpStep('位图谓词','EXPLAIN ANALYZE SELECT id,name FROM demo_students USE INDEX(demo_active) WHERE active=TRUE;','比较执行时堆访问与索引访问，不等同于OS完全没有data读取。'),
 helpStep('复合覆盖索引','CREATE INDEX demo_cover ON demo_students USING BPLUS(dept,id) INCLUDE(name,score);','前导列等值后可利用后列范围；仅所需列都覆盖时才省去回表。',{write:true}) ]},
 {id:'blob',chapter:'11',title:'大对象分块',intro:'普通槽记录不跨页，大对象保存在独立块中。这里生成16,385字节真实BLOB，输入仍低于SQL大小限制。',steps:[
 helpStep('建立大对象表','CREATE TABLE demo_blob(id INTEGER PRIMARY KEY,payload BLOB(65536));','BLOB不能作为搜索码或INCLUDE列。',{write:true}),
 helpStep('生成并写入16,385字节',()=>"INSERT INTO demo_blob VALUES(1,X'"+Array.from({length:16385},(_,i)=>(i%256).toString(16).padStart(2,'0')).join('').toUpperCase()+"');",'载入时生成完整十六进制SQL；预览省略字面量。',{write:true,preview:"-- 下方按钮生成完整16,385字节的INSERT语句。\n-- 字节序列00..FF循环填充，预览省略长字面量。"}),
 helpStep('读取真实值','SELECT id,payload FROM demo_blob;','返回的是数据库实际保存的字节，不是示意数据。'),helpStep('检查块与引用','VERIFY;','完整性校验检查BLOB块图和所属表。') ]},
 {id:'query',chapter:'12',title:'连接、分组与执行器',intro:'先准备课程表与连接键索引。物理连接算法可作受限强制对照；每次仍执行真实查询。',steps:[
 helpStep('课程数据准备','CREATE TABLE demo_courses(id INTEGER PRIMARY KEY,student_id INTEGER,course VARCHAR(40));','这里的student_id是普通列，未声明外键。',{write:true}),
 helpStep('插入课程',"INSERT INTO demo_courses VALUES(1,1,'数据库'),(2,1,'操作系统'),(3,2,'数据库');",'一对多连接应保留重复学生行。',{write:true}),
 helpStep('内表访问索引','CREATE INDEX demo_course_student ON demo_courses USING BPLUS(student_id);','IndexNestedLoop需要合法的参数化内表索引。',{write:true}),
 helpStep('真实HASH连接','EXPLAIN ANALYZE SELECT s.name,c.course FROM demo_students s JOIN demo_courses c ON s.id=c.student_id USE JOIN(HASH) ORDER BY s.id,c.id;','对照physical_actual与输入/输出行数。NESTED_LOOP/BLOCK_NESTED_LOOP/MERGE等按支持条件选择。'),
 helpStep('聚合与NULL','SELECT dept,COUNT(*) AS n_total,COUNT(score) AS n_score,AVG(score) AS avg_score FROM demo_students GROUP BY dept HAVING COUNT(*)>=1 ORDER BY dept;','COUNT(*)计全部行；COUNT(score)/AVG(score)只计非NULL值。') ]},
 {id:'optimizer',chapter:'13',title:'统计、成本选择、去相关与TopK',intro:'估算成本是教学模型，不是硬件耗时。计划同时报告估计和实际，采样统计也会标明范围与过期情况。',steps:[
 helpStep('收集事务可见统计','ANALYZE demo_students;','查看NULL、NDV、MCV、直方图及数据版本。统计会随事务提交/回滚。',{write:true}),
 helpStep('查看成本选路','EXPLAIN ANALYZE SELECT id,name FROM demo_students WHERE dept=\'计算机\' ORDER BY score DESC LIMIT 2;','TopK保留前K但仍检查候选输入；超内存或不适用时回退真实完整排序/外排。'),
 helpStep('受限EXISTS去相关','EXPLAIN ANALYZE SELECT s.id,s.name FROM demo_students s WHERE EXISTS(SELECT 1 FROM demo_courses c WHERE c.student_id=s.id);','安全单等值形状可按成本选择HashSemiJoin；其他形状保持真实Apply，不保证本小数据必选去相关。'),
 helpStep('物化聚合视图','CREATE MATERIALIZED VIEW demo_dept_mv WITH(refresh=\'immediate\') AS SELECT dept,COUNT(*) AS n,SUM(score) AS total FROM demo_students GROUP BY dept;','受支持的SP/聚合可增量维护；复杂形状真实FULL回退。',{write:true}),
 helpStep('读取视图状态','SHOW MATERIALIZED VIEWS;','检查freshness和依赖版本，不把精确绑定替代称为任意SQL视图包含推理。') ]},
 {id:'strict',chapter:'14',title:'ACID与保存点',intro:'默认严格表锁保留到事务结束。错误后按返回的transaction_state处理；aborted时只做回滚。',steps:[beginStep('当前会话','STRICT_2PL'),helpStep('保存点','SAVEPOINT before_change;','保存事务内页和元数据状态；不是向磁盘提交。'),helpStep('更新','UPDATE demo_students SET score=99 WHERE id=1;','当前事务看到自己的修改。',{write:true}),helpStep('回退到保存点','ROLLBACK TO before_change;','保留事务及锁，撤销保存点之后的更改。'),helpStep('结束事务','COMMIT;','成功回复由实际持久提交决定；连接中断造成结果未知时先核对数据，勿自动重试写入。') ]},
 {id:'mv2pl',chapter:'15',title:'MV2PL：旧读者与严格更新者',intro:'两个会话使用相同MV2PL协议。A固定BEGIN提交版本且不取用户表S；B用真实严格S/X更新。当前仅REDO_PRIVATE。',steps:[beginStep('会话A','MV2PL','READ ONLY',{modes:['REDO_PRIVATE']}),helpStep('A读取旧值','SELECT id,score FROM demo_students WHERE id=1;','保留A事务。'),beginStep('切换会话B','MV2PL','READ WRITE',{modes:['REDO_PRIVATE']}),helpStep('B修改','UPDATE demo_students SET score=score+1 WHERE id=1;','B持有真正表X到结束。',{write:true}),helpStep('B提交','COMMIT;','结束B后切回A。'),helpStep('A再次读取','SELECT id,score FROM demo_students WHERE id=1;','A保持旧值；新开的只读MV2PL事务看到新提交。'),helpStep('A结束','COMMIT;','旧读者结束后，版本历史可回收。') ]},
 {id:'mvto',chapter:'15',title:'MVTO：真实版本WTS/RTS冲突',intro:'整页版本的已提交MVTO变体，按WTS选择并提高RTS。物理页共享可能使不同逻辑行发生保守冲突；仅REDO_PRIVATE。',steps:[beginStep('会话A · 旧写者','MVTO','READ WRITE',{modes:['REDO_PRIVATE']}),helpStep('A先写但不提交','UPDATE demo_students SET score=score+1 WHERE id=1;','写入旧物理base也属于读取。',{write:true}),beginStep('切换会话B · 年轻读者','MVTO','READ ONLY',{modes:['REDO_PRIVATE']}),helpStep('B读取已提交旧值','SELECT id,score FROM demo_students WHERE id=1;','该版本RTS会被年轻读者提高。'),helpStep('切回A提交（预期拒绝）','COMMIT;','预期MVTO_RTS_CONFLICT在WAL之前拒绝旧写者，不代表系统失效。'),helpStep('A回滚','ROLLBACK;','清理A，再到B执行COMMIT结束读者。') ]},
 {id:'range',chapter:'15',title:'区间、空隙与死锁诊断',intro:'RECORD只支持Strict2PL下可提取的单表INTEGER主键AND范围。其他形状真实回退TABLE。空范围也能阻止落入gap的插入。',steps:[helpStep('idle设置RECORD','SET LOCK GRANULARITY RECORD;','先确认没有活动事务，当前恢复模式为REDO_PRIVATE。',{idle:true,modes:['REDO_PRIVATE']}),beginStep('当前会话','STRICT_2PL'),helpStep('读取主键区间','SELECT id,name FROM demo_students WHERE id>=10 AND id<20;','即使没有结果，也可持有实际区间S；右开界20不在该gap中。'),helpStep('看真实等待','SHOW LOCKS;','用另一同协议会话插入id=15会等待；锁超时/死锁都需要回滚。'),helpStep('结束范围事务','ROLLBACK;','释放区间与意向锁。'),helpStep('恢复TABLE默认','SET LOCK GRANULARITY TABLE;','切到其他协议前先在idle恢复TABLE。',{idle:true}) ]},
 {id:'other-protocols',chapter:'15',title:'其他隔离与协议入口',intro:'同一时段活动事务应处于相同协议epoch。以下BEGIN互为替代，每个都必须单独结束后再用下一个。RC每语句刷新，Snapshot固定视图；RU请求安全提升为RC。',steps:[beginStep('单独实验','SNAPSHOT ISOLATION LEVEL READ COMMITTED','READ ONLY',{modes:['REDO_PRIVATE']}),helpStep('结束RC','COMMIT;','先结束，再切其他协议。'),beginStep('单独实验','SNAPSHOT','READ ONLY',{modes:['REDO_PRIVATE']}),helpStep('结束Snapshot','COMMIT;','固定版本不等于保证全部SQL串行化。'),beginStep('单独实验','OCC_SERIALIZABLE','READ WRITE',{modes:['REDO_PRIVATE']}),helpStep('结束OCC','ROLLBACK;','OCC会在提交前验证读集及页/表版本。'),beginStep('单独实验','TIMESTAMP','READ WRITE',{modes:['REDO_PRIVATE']}),helpStep('结束TS','ROLLBACK;','TS/Thomas是粗粒度实验；Thomas仅真正盲写可采用过时写忽略规则。') ]},
 {id:'recovery',chapter:'16',title:'WAL、检查点与恢复身份',intro:'恢复模式是数据目录持久配置，不能通过一条BEGIN切换。REDO_PRIVATE不刷未提交页；ARIES_PAGE允许STEAL并用物理UNDO/CLR恢复，当前只配Strict2PL TABLE。',steps:[helpStep('查看角色与时间线','SHOW RECOVERY;','PRIMARY/RESTORED_READ_ONLY/STANDBY、fence epoch与timeline以实际返回为准。'),helpStep('查看WAL状态','SHOW WAL;','提交确认、同步和页发布有明确边界。'),helpStep('ARIES日志记录','SHOW WAL RECORDS LIMIT 40;','ARIES_PAGE显示实际物理UPDATE/COMMIT/CLR；ARIES_LOGICAL另含logical_records，其LSN与物理LSN独立；REDO_PRIVATE明确拒绝。',{modes:['ARIES_PAGE','ARIES_LOGICAL']}),helpStep('SQL检查点','CHECKPOINT;','REDO模式有界等待活动事务，超时BUSY；ARIES为自己的模糊检查点流程。',{idle:true}) ]},
 {id:'backup',chapter:'16',title:'物理备份、归档与只读备库',intro:'此阶段备份/归档/LSN恢复/异步备库只支持REDO_PRIVATE。备份标签唯一；恢复写入全新目录。晋升操作需在独立恢复/备库实例中执行。',steps:[helpStep('启用归档','SET ARCHIVING ON;','idle维护操作，保存已完成WAL代供恢复。',{idle:true,modes:['REDO_PRIVATE'],write:true}),helpStep('建立课程备份',"BACKUP DATABASE AS 'lesson01';",'已有标签应改名；维护会等待活动事务，超时返回BUSY。',{idle:true,modes:['REDO_PRIVATE'],write:true}),helpStep('归档有效提交日志','ARCHIVE WAL;','只接受完整、校验有效的日志段，失败不伪造恢复通过。',{idle:true,modes:['REDO_PRIVATE'],write:true}),helpStep('只读备库身份','SHOW RECOVERY;','只读恢复/备库角色不能靠BEGIN READ WRITE解除。'),helpStep('专用备库晋升','PROMOTE STANDBY;','仅在独立专用备库实验执行；同用户同机fence，异步fork可能遗漏未接收提交，旧提交回复可晚到。',{idle:true,modes:['REDO_PRIVATE'],standbyOnly:true}) ]},
 {id:'tree-pending',chapter:'15',title:'有界TREE：静态逻辑行树',pending:'tree',intro:'已实现的REDO_PRIVATE有界机制，按下列单条SQL顺序实验。首个节点X锁可自由选择，后续锁须持有父节点；未写节点可早释，已释节点不能重获，写节点持至事务结束。仅独立HEAP、单INTEGER主键及不同INTEGER父列，单拓扑最多10万节点、活动合计20万节点。不是等待图，也不是可任意更新的动态树。',steps:[
 helpStep('准备静态树表','CREATE TABLE demo_tree(id INTEGER PRIMARY KEY,parent INTEGER,v INTEGER);','准备阶段使用普通自动事务；唯一NULL根，其他父节点存在，不能有环。',{idle:true,write:true,modes:['REDO_PRIVATE']}),
 helpStep('准备父子行','INSERT INTO demo_tree VALUES(1,NULL,10),(2,1,20),(3,1,30);','树是实际表中的逻辑行父子关系。',{idle:true,write:true,modes:['REDO_PRIVATE']}),
 beginStep('当前会话','TREE','READ WRITE',{modes:['REDO_PRIVATE']}),
 helpStep('打开静态拓扑','TREE OPEN demo_tree PARENT parent;','活动TREE期间禁止插删节点、修改主键/父列、复杂查询和保存点。',{modes:['REDO_PRIVATE']}),
 helpStep('首先锁根','TREE LOCK 1;','真实节点X锁；首锁可选任意节点。',{modes:['REDO_PRIVATE']}),
 helpStep('持父锁后锁子','TREE LOCK 2;','取得子节点前仍持有父节点1。',{modes:['REDO_PRIVATE']}),
 helpStep('早释未写根','TREE UNLOCK 1;','本事务不能再重获节点1；节点2仍由本事务保护。',{modes:['REDO_PRIVATE']}),
 helpStep('受保护单主键更新','UPDATE demo_tree SET v=21 WHERE id=2;','节点2写过后不能提前解锁。不同节点共享物理页的写者仍可能发生整页FCW中止。',{write:true,modes:['REDO_PRIVATE']}),
 helpStep('查看TREE实际状态','TREE STATUS;','核对持有与已释放节点；未持锁节点的读写会明确拒绝。',{modes:['REDO_PRIVATE']}),
 helpStep('结束TREE事务','COMMIT;','释放剩余锁；若事务aborted则改用ROLLBACK。',{modes:['REDO_PRIVATE']}) ]},
 {id:'logical-pending',chapter:'16',title:'有界逻辑UNDO：A回滚保留B提交',pending:'logicalUndo',intro:'已实现ARIES_LOGICAL实验模式，须在独立新目录用--recovery-mode ARIES_LOGICAL启动；BEGIN不能切换模式。真实逻辑行逆操作与物理mini事务分离，默认ARIES_PAGE仍只用Strict2PL TABLE。限制：全局最多3个尚未提交/撤销的行操作、128行/表、1 KiB行、1024页，每操作预留最多两次自动逆操作尝试，预算耗尽fail-closed。以下两键可在同一物理页；必须由INSPECT真实RID证明同页，不能只由结果推断。',steps:[
 helpStep('准备逻辑UNDO表','CREATE TABLE demo_logical(id INTEGER PRIMARY KEY,v VARCHAR(40));','仅独立HEAP、单INTEGER主键、1—8标量列；DDL用独立自动事务。',{idle:true,write:true,modes:['ARIES_LOGICAL']}),
 helpStep('准备两键','INSERT INTO demo_logical VALUES(1,\'原值一\'),(2,\'原值二\');','先完成准备，再开始两个会话。',{idle:true,write:true,modes:['ARIES_LOGICAL']}),
 beginStep('会话A','STRICT_2PL','READ WRITE',{modes:['ARIES_LOGICAL']}),
 helpStep('A修改键1','UPDATE demo_logical SET v=\'甲尚未提交\' WHERE id=1;','保留A的活动事务，不要提交。实际单PK谓词使用RECORD保护。',{write:true,modes:['ARIES_LOGICAL']}),
 beginStep('切换会话B','STRICT_2PL','READ WRITE',{modes:['ARIES_LOGICAL']}),
 helpStep('B修改键2','UPDATE demo_logical SET v=\'乙已提交\' WHERE id=2;','两个不同键；此时全局未完成行操作数为2。',{write:true,modes:['ARIES_LOGICAL']}),
 helpStep('B提交','COMMIT;','确认B提交成功后，手动切回A。',{modes:['ARIES_LOGICAL']}),
 helpStep('A回滚','ROLLBACK;','真正执行行逆操作，保留B已经提交的另一个键。',{modes:['ARIES_LOGICAL']}),
 helpStep('核对两键结果','SELECT id,v FROM demo_logical ORDER BY id;','应为原值一、乙已提交。整表读使用真正粗表S锁；须在A回滚后执行。',{modes:['ARIES_LOGICAL']}),
 helpStep('核对实际行位置','INSPECT TABLE demo_logical;','检查实际RID页号是否相同；查看逻辑/物理LSN时不要跨编号空间比较大小。',{modes:['ARIES_LOGICAL']}) ]},
 {id:'reuse-pending',chapter:'13',title:'有界同表自连接消除',pending:'selfJoin',intro:'已实现同表单INTEGER非NULL主键双别名等值自连接消除，按成本和安全条件选择；不是任意自连接或外键消除。使用真实EXPLAIN ANALYZE核对rewrite决策、实际访问和返回值，不把SQL文本相似当作已消除。',steps:[
 helpStep('准备唯一主键表','CREATE TABLE demo_reuse(id INTEGER PRIMARY KEY,v INTEGER);','INTEGER PRIMARY KEY提供唯一性与非NULL前提。',{idle:true,write:true}),
 helpStep('准备数据','INSERT INTO demo_reuse VALUES(1,10),(2,20),(3,30);','两个别名指向同一张实际表。',{write:true}),
 helpStep('真实自连接消除计划','EXPLAIN ANALYZE SELECT a.id,b.v FROM demo_reuse a JOIN demo_reuse b ON a.id=b.id WHERE a.id>=1 ORDER BY a.id;','观察同表PK证明、候选成本和实际选路；结果来自执行器，不是静态示意。') ]},
 {id:'shared-ready',chapter:'13',title:'真实批量共享扫描与单SQL的区别',pending:'sharedScan',intro:'已实现execute_shared_batch、POST /api/v1/sessions/{id}/batch与edudb batch --file。批次为idle、Strict2PL TABLE、只读的同表受限SELECT，最多16条、总SQL 256 KiB；支持REDO_PRIVATE/ARIES_PAGE，拒绝ARIES_LOGICAL。网页编辑器仍一次执行一条SQL，下面按钮仅装入普通SELECT，不会调用batch。CLI示例见下方命令区。',steps:[
 helpStep('单SQL行结果对照','SELECT id,v FROM demo_reuse WHERE id>=2 ORDER BY id;','这是一次普通SELECT，可与batch对应项逐值比较；不据此宣称跨查询共享。'),
 helpStep('单SQL聚合对照','SELECT COUNT(*),SUM(v) FROM demo_reuse;','CLI batch中可放这两条SELECT。核对metrics.shared_scan的base_scan_opens/base_rows与spool_reader_opens/临时I/O：一次基表游标加多个真实spool读者，不是零I/O，也不是把单SQL双别名复制称作批量共享。') ]}
];
const helpCommands=[
 ['另建ARIES_LOGICAL实验目录',String.raw`.\edudb.exe serve --data .\data-v4-logical --web .\web --port 8768 --frames 8 --recovery-mode ARIES_LOGICAL`,'新目录持久配置；仅有界Strict2PL逻辑行实验。先准备两键，按上方A/B步骤操作。备份、归档、PITR、主备和共享batch在此模式明确拒绝。'],
 ['批量共享扫描（CLI）',String.raw`.\edudb.exe batch --url http://127.0.0.1:8765 --file .\shared-demo.sql`,'先在默认演示库建立demo_reuse。shared-demo.sql保存两条：SELECT id,v FROM demo_reuse WHERE id>=2 ORDER BY id; 与 SELECT COUNT(*),SUM(v) FROM demo_reuse;。batch使用专用API；shell/run与网页逐条执行SQL不会自动变成共享批次。查看基表与spool两类计数并核对每项真实结果。'],
 ['新建默认演示库',String.raw`.\edudb.exe serve --data .\data-v4-demo --web .\web --port 8765`,'在PowerShell启动后访问 http://127.0.0.1:8765。CLI shell/run与网页连接同一服务进程。'],
 ['另建ARIES_PAGE目录',String.raw`.\edudb.exe serve --data .\data-v4-aries --web .\web --port 8766 --recovery-mode ARIES_PAGE --aries-steal true --aries-force-at-commit false`,'这是新目录的持久配置。重开时沿用目录配置；更改恢复模式须逻辑导出/导入另一新目录。访问8766端口，ARIES当前只用Strict2PL TABLE。'],
 ['离线迁移旧控制格式',String.raw`.\edudb.exe migrate-v3 --source "<已关闭的旧库目录>" --data ".\data-v4-migrated"`,'源库须关闭、目标不存在。旧控制格式3只读查看；迁移在副本恢复/逻辑核对后发布新UUID目录，源文件不原地升级。'],
 ['恢复到全新目录',String.raw`.\edudb.exe restore --source "<完整备份目录>" --data ".\data-v4-restored" --archive "<有效归档清单>" --target-lsn <有效提交LSN>`,'替换所有占位符；目标必须不存在。恢复实例保持只读角色，专用备库实验再按手册建立接收/晋升。'],
 ['软件RAID5新目录',String.raw`.\edudb.exe serve --data .\data-v4-raid5 --web .\web --port 8767 --data-layout RAID5 --media-members 4`,'受管文件冗余/校验/重建是真实文件I/O；未宣称独立硬盘硬件吞吐或断电通过。']
];
function helpNode(tag,text,cls){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;}
function helpNotice(text,error=false){const p=$('help-action-message');if(p){p.textContent=text;p.className='help-action'+(error?' error':'');}}
function helpCurrentMode(){const s=helpDiagnostics?.storage;return s?.recovery_mode||s?.mode||s?.wal?.mode||(s?.private_page_limit_bytes!==undefined?'REDO_PRIVATE':null);}
function refreshHelpRuntime(d){if(d)helpDiagnostics=d;const el=$('help-runtime');if(!el)return;const s=sessions.get(current),storage=helpDiagnostics?.storage,identity=storage?.recovery_identity;el.textContent='当前实例：'+(helpCurrentMode()||'尚未报告恢复模式')+'　角色：'+(identity?.role||'尚未报告')+'　会话：'+(s?.state||'未连接')+(s?.transactionOptions?.protocol?' / '+s.transactionOptions.protocol:'')+(storage?.read_only?' / 旧格式只读':'')+(identity?.fenced?' / 已fenced':'');}
function loadHelpStep(guide,step){
 const s=sessions.get(current),mode=helpCurrentMode(),role=helpDiagnostics?.storage?.recovery_identity?.role;
 if(guide.pending&&!helpRelease[guide.pending]){helpNotice('此项等待整机验收，暂不启用载入。',true);return;}
 if(s&&(s.busy||s.closing||s.invalid)){helpNotice('先等待当前请求完成，或核对未知结果后重新连接。',true);return;}
 if(step.idle&&s&&s.state!=='idle'){helpNotice('此步骤需要idle会话。先在当前会话COMMIT或ROLLBACK，或创建另一个独立会话；不要嵌套BEGIN或在活动事务中混协议。',true);return;}
 if(step.modes&&mode&&!step.modes.includes(mode)){helpNotice('当前持久恢复模式为'+mode+'，此步骤适用'+step.modes.join(' / ')+'。使用匹配的新目录/服务，BEGIN不能切换目录恢复模式。',true);return;}
 if(step.write&&(helpDiagnostics?.storage?.read_only||(role&&role!=='PRIMARY')||helpDiagnostics?.storage?.recovery_identity?.fenced)){helpNotice('当前实例为只读或已fenced。BEGIN READ WRITE不能解除实例角色；请使用匹配的可写演示库。',true);return;}
 if(step.standbyOnly&&role&&role!=='STANDBY'&&role!=='RESTORED_READ_ONLY'){helpNotice('晋升示例仅用于独立恢复/备库实例，请先核对SHOW RECOVERY。',true);return;}
 const text=typeof step.sql==='function'?step.sql():step.sql;$('sql').value=text;if(s)s.sql=text;$('sql').dispatchEvent(new Event('input',{bubbles:true}));helpNotice('已载入“'+step.label+'”。仅填编辑器，尚未执行。');message('示例已载入，点击“执行 SQL”发送当前一条语句。',false);$('sql').scrollIntoView({block:'center',behavior:'auto'});$('sql').focus();
}
function helpJump(tab,inspect){const target=document.querySelector('.tabs [data-tab="'+tab+'"]');if(inspect){$('inspect-table').value=inspect.table;$('inspect-index').value=inspect.index||'';}if(target)target.click();helpNotice(inspect?'已填入结构参数；先准备真实数据，再点击“读取真实结构”。':'已切换到对应功能。');}
function renderHelpGuides(){
 const area=$('help-guides');if(!area)return;area.replaceChildren();
 for(const g of helpGuides.filter(x=>helpCategory==='all'||x.chapter===helpCategory||(helpCategory==='basic'&&x.chapter==='基础'))){const card=helpNode('article',undefined,'help-card');card.id='help-guide-'+g.id;const head=helpNode('div',undefined,'help-card-head');head.append(helpNode('span',g.chapter==='基础'?'基础功能':'第'+g.chapter+'章','help-kicker'),helpNode('span',g.pending&&!helpRelease[g.pending]?'候选 · 待联合验收':'真实内核功能','help-badge'+(g.pending&&!helpRelease[g.pending]?' pending':'')));card.append(head,helpNode('h3',g.title),helpNode('p',g.intro));
  for(const step of g.steps){const box=helpNode('details',undefined,'help-step');box.append(helpNode('summary',step.label));box.append(helpNode('pre',step.preview||(typeof step.sql==='function'?'载入时生成完整SQL。':step.sql)),helpNode('p',step.observe,'help-observe'));const load=helpNode('button','载入这一条SQL','secondary');load.type='button';load.dataset.helpLoad=g.id;load.disabled=!!g.pending&&!helpRelease[g.pending];load.onclick=()=>loadHelpStep(g,step);box.append(load);card.append(box);}
  if(g.inspect){const b=helpNode('button','填入结构查看参数','secondary');b.type='button';b.onclick=()=>helpJump('ch11',g.inspect);card.append(b);}area.append(card);
 }
 $('help-category-label').textContent=helpCategory==='all'?'全部路线':helpCategory==='basic'?'基础操作':'第'+helpCategory+'章';
 document.querySelectorAll('[data-help-category]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.helpCategory===helpCategory)));
}
function renderHelpNavigation(){
 const nav=$('help-chapters');if(!nav)return;for(const [id,label]of [['basic','基础SQL'],['10','10 · 存储'],['11','11 · 索引'],['12','12 · 执行'],['13','13 · 优化'],['14','14 · 事务'],['15','15 · 并发'],['16','16 · 恢复'],['all','全部路线']]){const b=helpNode('button',label);b.type='button';b.dataset.helpCategory=id;b.onclick=()=>{helpCategory=id;renderHelpGuides();};nav.append(b);}
 for(const [title,command,note]of helpCommands){const d=helpNode('details',undefined,'help-command');d.append(helpNode('summary',title),helpNode('pre',command),helpNode('p',note));$('help-commands').append(d);}
 document.querySelectorAll('[data-help-jump]').forEach(b=>b.onclick=()=>helpJump(b.dataset.helpJump));renderHelpGuides();refreshHelpRuntime();
}

$('execute').onclick=()=>execute();$('new-session').onclick=createSession;$('reconnect').onclick=reconnect;$('refresh-schema').onclick=refreshDiagnostics;$('refresh-diag').onclick=refreshDiagnostics;
$('sql').oninput=()=>{if(sessions.has(current))sessions.get(current).sql=$('sql').value;};
document.querySelectorAll('[data-sql]').forEach(b=>b.onclick=()=>execute(b.dataset.sql));
document.querySelectorAll('[data-tab]').forEach(b=>b.onclick=()=>{document.querySelectorAll('[data-tab],.tab-content').forEach(x=>x.classList.remove('active'));b.classList.add('active');$(b.dataset.tab).classList.add('active');if(b.dataset.tab==='kernel')refreshDiagnostics();});
$('sql').onkeydown=e=>{if(e.key==='Enter'&&(e.ctrlKey||e.metaKey)){e.preventDefault();execute();}};
$('checkpoint').onclick=async()=>{try{const j=await api('/checkpoint','POST',{});const ok=j.ok===true||j.status==='PASS';message(ok?'检查点完成。':j.status==='BUSY'?'检查点 BUSY：仍有活动事务，请先结束事务再手动重试。':sqlError(j.error||{}),!ok);await refreshDiagnostics();}catch(e){message('检查点请求失败：'+e.message+'。请核对实际状态后再决定是否重试。',true);connection(false);}};
window.addEventListener('pagehide',()=>{leaving=true;for(const id of sessions.keys())pendingCleanup.add(id);rememberOwned();for(const id of pendingCleanup)fetch('/api/v1/sessions/'+encodeURIComponent(id),{method:'DELETE',keepalive:true}).catch(()=>{});releaseOwnership?.();releaseOwnership=null;});
window.addEventListener('pageshow',e=>{if(e.persisted){leaving=false;sessions.clear();current=null;claimOwnership().then(createSession).then(refreshDiagnostics);}});
renderHelpNavigation();updateControls();claimOwnership().then(createSession).then(refreshDiagnostics).catch(e=>{message('页面会话初始化失败：'+e.message,true);connection(false);});
