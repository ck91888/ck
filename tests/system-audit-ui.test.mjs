import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import vm from 'node:vm';
const source=name=>fs.readFileSync(new URL('../'+name,import.meta.url),'utf8');

test('human-readable change notices handle historical malformed diffs and escape customer content',()=>{
 const c={};c.window=c;vm.runInNewContext(source('shared/outbound-changes.js'),c);const ui=c.CKOutboundChanges;
 assert.equal(ui.diff({need_id:'NEED-old'},x=>x,(_k,v)=>v),'');
 assert.equal(ui.diff({foo:{from:'',to:''}},x=>x,(_k,v)=>v),'');
 assert.match(ui.title({change_type:'material_update',summary_text:'出库数量与单位已调整'}),/数量/);
 const html=ui.diff({shipping_quantity:{from:'2 箱',to:'2 托'}},x=>x,(_k,v)=>v);assert.match(html,/2 箱/);assert.match(html,/2 托/);assert.ok(!html.includes('shipping_quantity'));
 assert.ok(!ui.summary({change_type:'material_update',summary_text:'<img onerror=alert(1)>'}).includes('<img'));
});

test('read coalescing shares only simultaneous reads and never caches results or suppresses mutations',async()=>{
 const pending=[],c={URL,Response,location:new URL('https://fixture.test/002/'),document:{documentElement:{classList:{add(){}}},addEventListener(){}},CK_SOP_ROLLOUT:{staging:true},SOP_API:'/api',fetch:(url,init)=>new Promise((resolve,reject)=>pending.push({body:JSON.parse(init.body),resolve,reject}))};c.window=c;
 vm.runInNewContext(source('shared/sop-session.js'),c);
 const call=(action='sop_get')=>c.fetch('/api',{method:'POST',body:JSON.stringify({action,id:'A'})});
 const a=call(),b=call();assert.equal(pending.length,1);pending[0].resolve(Response.json({value:1}));assert.deepEqual(await (await a).json(),{value:1});assert.deepEqual(await (await b).json(),{value:1});
 const fresh=call();assert.equal(pending.length,2);pending[1].resolve(Response.json({value:2}));assert.equal((await (await fresh).json()).value,2);
 const readBefore=call(),mutation=call('sop_need_update'),readAfter=call();assert.equal(pending.length,5);for(const p of pending.slice(2))p.resolve(Response.json({ok:true}));await Promise.all([readBefore,mutation,readAfter]);
 const x=call('sop_need_update'),y=call('sop_need_update');assert.equal(pending.length,7);pending.slice(5).forEach(p=>p.resolve(Response.json({ok:true})));await Promise.all([x,y]);
 const failed=call();pending[7].reject(Error('offline'));await assert.rejects(failed,/offline/);const retry=call();assert.equal(pending.length,9);pending[8].resolve(Response.json({ok:true}));await retry;
});

test('job polling starts after an idle screen gains a task, does not overlap, and skips hidden pages',async()=>{
 const full=source('001/app.js'),s=full.slice(full.indexOf('function startJobPoll(type)'),full.indexOf('function renderWorkers(',full.indexOf('function startJobPoll(type)')));let tick,release,count=0;
 const refresh=()=>{count++;return new Promise(resolve=>release=resolve);};const c={_pollTimer:null,_activeJobId:null,document:{hidden:false},setInterval:fn=>{tick=fn;return 1;},clearInterval(){}};
 for(const name of ['Unload','Load','Inbound','InboundReturn','Generic','Pick','Bulk','ImportDelivery','VerifyScan'])c['refresh'+name+'Workers']=refresh;
 vm.runInNewContext(s,c);c.startJobPoll('bulk');await tick();assert.equal(count,0);c._activeJobId='A';const first=tick();await tick();assert.equal(count,1);release();await first;c.document.hidden=true;await tick();assert.equal(count,1);c.document.hidden=false;c._activeJobId='B';const next=tick();assert.equal(count,2);release();await next;
});

test('outbound materials reuse detail attachments and discard another orders late file response',async()=>{
 const full=source('001/app.js'),s=full.slice(full.indexOf('async function renderOutboundMaterials('),full.indexOf('// ===== P1-8/P1-9',full.indexOf('async function renderOutboundMaterials(')));const box={dataset:{},innerHTML:''};let release,calls=0;
 const c={document:{getElementById:()=>box},api:()=>{calls++;return new Promise(resolve=>release=resolve);},esc:x=>String(x),V2_API:'/001/api'};vm.runInNewContext(s,c);
 const old=c.renderOutboundMaterials('A','files');await c.renderOutboundMaterials('B','files',[{file_name:'Only B',file_key:'B.pdf',attachment_category:'outbound_material'}]);assert.equal(calls,1);assert.match(box.innerHTML,/Only B/);
 release({ok:true,items:[{file_name:'Stale A',file_key:'A.pdf',attachment_category:'outbound_material'}]});await old;assert.match(box.innerHTML,/Only B/);assert.ok(!box.innerHTML.includes('Stale A'));
});
