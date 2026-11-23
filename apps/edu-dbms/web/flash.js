'use strict';
(function(global){
 const defaults={program_bytes:4096,erase_bytes:262144,spare_ratio:0.25};
 function simulate(trace,options={}){
  const parameters={...defaults,...options},unit=parameters.program_bytes,blockBytes=parameters.erase_bytes,spare=parameters.spare_ratio;
  if(!Number.isSafeInteger(unit)||!Number.isSafeInteger(blockBytes)||unit<1||blockBytes%unit||blockBytes/unit<2||spare<0||spare>=.9)throw Error('Invalid flash model parameters');
  const blockPages=blockBytes/unit,writes=[],logical=new Map();let sourceBytes=0;
  for(const event of trace||[]){if(event.role!=='data'||event.operation!=='write'||event.failed)continue;const bytes=Number(event.bytes);if(!Number.isSafeInteger(bytes)||bytes<0)throw Error('Invalid trace byte count');if(!bytes)continue;const start=BigInt(event.offset??0),end=start+BigInt(bytes);if(start<0)throw Error('Invalid trace offset');sourceBytes+=bytes;
   for(let page=start/BigInt(unit);page*BigInt(unit)<end;page++){const address=page.toString();if(!logical.has(address))logical.set(address,logical.size);writes.push(logical.get(address));if(writes.length>500000)throw Error('Model limited to 500000 program-unit writes');}
  }
  if(!writes.length)throw Error('No successful data-file write calls in the collected real trace');
  // The small-workset reserve rounds capacity upward so GC can copy a whole
  // live block without overwriting it. Effective spare is reported explicitly.
  const logicalPages=logical.size,blocks=Math.max(2,Math.ceil(logicalPages/(1-spare)/blockPages),Math.ceil((logicalPages+blockPages)/blockPages));
  const physicalPages=blocks*blockPages,pages=new Int32Array(physicalPages);pages.fill(-1);const mapping=new Map();let head=0,free=physicalPages,programs=0,copies=0,erases=0,collections=0;
  function program(lpn,excluded=-1){for(let checked=0;checked<physicalPages;checked++){const at=head;head=(head+1)%physicalPages;if(pages[at]!==-1||Math.floor(at/blockPages)===excluded)continue;pages[at]=lpn;mapping.set(lpn,at);--free;++programs;return at;}throw Error('Flash model exhausted reserved free space');}
  function collect(){let victim=-1,best=blockPages+1;
   for(let block=0;block<blocks;block++){let valid=0,invalid=0,available=0;for(let i=block*blockPages;i<(block+1)*blockPages;i++){if(pages[i]===-1)available++;else if(pages[i]===-2)invalid++;else valid++;}if(!available&&invalid&&valid<=free&&valid<best){best=valid;victim=block;}}
   if(victim<0)return false;const begin=victim*blockPages,end=begin+blockPages;
   const live=[];for(let i=begin;i<end;i++)if(pages[i]>=0)live.push([i,pages[i]]);
   for(const [old,lpn]of live){program(lpn,victim);pages[old]=-2;++copies;}
   for(let i=begin;i<end;i++)pages[i]=-1;free+=blockPages;++erases;++collections;return true;
  }
  for(const lpn of writes){while(free<=blockPages&&collect()){}const old=mapping.get(lpn);program(lpn);if(old!==undefined)pages[old]=-2;}
  const final=[...mapping.entries()].sort((a,b)=>a[0]-b[0]);if(new Set(final.map(x=>x[1])).size!==final.length||final.some(([lpn,at])=>pages[at]!==lpn))throw Error('Model map invariant failed');
  return {kind:'trace-driven flash model; not host SSD measurement',parameters:{...parameters,block_pages:blockPages,reserve_blocks:1,garbage_collection:'greedy fewest live pages among closed blocks; deterministic first-block tie'},source:{data_write_calls:(trace||[]).filter(x=>x.role==='data'&&x.operation==='write'&&!x.failed).length,data_write_bytes:sourceBytes,program_unit_writes:writes.length,logical_pages:logicalPages,address_mapping:'Actual file page addresses compacted by first occurrence; hotness/order preserved'},model:{physical_blocks:blocks,physical_pages:physicalPages,effective_spare_ratio:1-logicalPages/physicalPages,host_program_bytes:writes.length*unit,program_bytes:programs*unit,gc_copy_bytes:copies*unit,erase_count:erases,collections,model_WA:programs/writes.length,free_pages:free,live_pages:mapping.size},final_mapping:final};
 }
 const collected=[];let discarded=0,truncatedRequests=0;
 global.EduDBTraceCollector={add(io){if(!io?.trace)return;if(io.trace_truncated)truncatedRequests++;for(const event of io.trace){if(collected.length>=100000){discarded++;continue;}collected.push({...event});}},clear(){collected.length=0;discarded=0;truncatedRequests=0;},snapshot(){return {trace:collected.map(x=>({...x})),discarded,truncated_requests:truncatedRequests};}};
 global.EduDBFlashModel={simulate,defaults};if(typeof module!=='undefined'&&module.exports)module.exports={simulate,defaults};
})(typeof window==='undefined'?globalThis:window);
