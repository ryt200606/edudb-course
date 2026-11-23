'use strict';
window.EduDBDiagnostics={render(storage){
 const section=document.createElement('section');section.className='buffer-view';
 if(storage.io)section.append(window.EduDBIO.render(storage.io));
 const title=document.createElement('h2');title.textContent='当前缓冲页';section.append(title);
 const frames=document.createElement('div');frames.className='page-grid';
 const types={1:'格式',2:'目录',3:'表描述',4:'堆记录',5:'B+ 树',6:'退役页',7:'统一访问方法页'};
 for(const p of storage.pages||[]){const box=document.createElement('div');box.className='buffer-page'+(p.dirty?' dirty':'');const name=document.createElement('strong');name.textContent='Page '+p.id;const info=document.createElement('span');info.textContent=(types[p.type]||'未知')+' · LSN '+p.lsn;const detail=document.createElement('small');detail.textContent=p.slots+' 槽 · '+(p.dirty?'脏页':'已刷盘');box.append(name,info,detail);frames.append(box);}section.append(frames);
 if(storage.index_nodes?.length){const h=document.createElement('h2');h.textContent='缓存中的索引节点';section.append(h);const note=document.createElement('p');note.className='section-description';note.textContent='显示当前驻留缓冲池的节点，最多64个。已淘汰节点不在其中，因此不代表完整索引树。';section.append(note);
 for(const n of storage.index_nodes){const box=document.createElement('div');box.className='index-node';const label=document.createElement('strong');label.textContent='Page '+n.page_id+' · '+(n.leaf?'叶节点':'内部节点');const keys=document.createElement('div');keys.className='index-keys';for(const k of (n.keys||[]).slice(0,16)){const span=document.createElement('span');span.textContent=Array.isArray(k)?String(k[0]):String(k);if(Array.isArray(k))span.title='RID: page='+k[1]+', slot='+k[2]+', generation='+k[3];keys.append(span);}if((n.keys||[]).length>16){const more=document.createElement('span');more.textContent='… 共'+n.keys.length+'个';keys.append(more);}const links=document.createElement('small');links.textContent=n.leaf?'下一叶页：'+(n.next||'无'):'子页：'+(n.children||[]).join('，');box.append(label,keys,links);section.append(box);}}
 return section;
}};
window.EduDBIO={render(io){
 const section=document.createElement('section');section.className='io-view';const title=document.createElement('h2');title.textContent='文件 I/O · '+(io.scope||'本次请求');section.append(title);
 const note=document.createElement('p');note.className='section-description';note.textContent='以下是操作系统文件读写/同步调用，可能命中操作系统缓存；不能等同物理磁盘 I/O。逻辑页访问和缓冲命中另行统计。';section.append(note);
 const wrap=document.createElement('div');wrap.className='table-wrap';const table=document.createElement('table'),head=document.createElement('tr');for(const name of ['文件角色','读次数','读字节','写次数','写字节','同步次数','同步耗时 ns','失败']){const cell=document.createElement('th');cell.textContent=name;head.append(cell);}table.append(head);
 for(const [role,c]of Object.entries(io.files||{})){const row=document.createElement('tr');for(const value of [role,c.read_calls,c.read_bytes,c.write_calls,c.write_bytes,c.sync_calls,c.sync_ns,c.failures]){const cell=document.createElement('td');cell.textContent=value===undefined?'未提供':String(value);row.append(cell);}table.append(row);}wrap.append(table);section.append(wrap);
 if(io.trace){const details=document.createElement('details'),summary=document.createElement('summary'),pre=document.createElement('pre');summary.textContent=`本次调用追踪 · 返回${io.trace.length}项 / 总${io.trace_total??io.trace.length}项${io.trace_truncated?'（已截断）':''}`;pre.textContent=JSON.stringify(io.trace,null,2);details.append(summary,pre);section.append(details);}return section;
}};
