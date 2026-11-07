// Deterministic clock: the total budget includes queueing, backoff and acknowledgements.
import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';import vm from 'node:vm';import test from 'node:test';
const source=readFileSync(new URL('../shared/material-upload.js',import.meta.url),'utf8');
const flush=async()=>{for(let i=0;i<20;i++)await Promise.resolve();};
function fixture(){
 let now=0,sequence=0,active=0;const timers=new Map(),sent=[],window={CKSession:{user:{id:'synthetic-owner',role:'manager',scope:'office',operation_context:{id:'synthetic-context'}}}},location=new URL('http://localhost/002/');
 const schedule=(fn,ms=0)=>{const id=++sequence;timers.set(id,{at:now+ms,fn});return id;},clear=id=>timers.delete(id);
 class XHR{constructor(){this.upload={};}open(){}setRequestHeader(){}send(body){this.body=body;this.live=true;active++;sent.push(this);this.timer=schedule(()=>this.finish('timeout'),this.timeout);}finish(kind='load',body={ok:true}){if(!this.live)return;this.live=false;active--;clear(this.timer);this.status=200;this.responseText=JSON.stringify(body);this['on'+kind]?.();}abort(){this.finish('abort');}}
 vm.runInNewContext(source,{window,location,URL,FormData,AbortController,performance:{now:()=>now},XMLHttpRequest:XHR,setTimeout:schedule,clearTimeout:clear,Promise});
 Object.assign(window.CKMaterialUpload.limits,{totalMs:200,idleMs:1000,retryMs:[0,0]});
 async function tick(ms){const end=now+ms;await flush();for(;;){const next=[...timers].filter(([,v])=>v.at<=end).sort((a,b)=>a[1].at-b[1].at||a[0]-b[0])[0];if(!next)break;now=next[1].at;timers.delete(next[0]);next[1].fn();await flush();}now=end;await flush();}
 const body=id=>{const data=new FormData();for(const[k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:'cargo_draft',related_doc_id:'synthetic-draft',client_req_id:id,file:new File(['synthetic'],'synthetic.pdf')}))data.set(k,v);return data;};
 const track=promise=>{const result={};result.done=promise.then(value=>{result.value=value;result.at=now;},error=>{result.error=error;result.at=now;});return result;};
 return {api:window.CKMaterialUpload,sent,tick,body,track,window,get active(){return active;},get timers(){return timers.size;}};
}
test('200ms is one total budget, not three fresh 200ms request timeouts',async()=>{
 const f=fixture(),r=f.track(f.api.send(f.body('one-key')));await f.tick(200);assert.ok(r.error,'must settle at the absolute deadline');assert.equal(r.error.deadlineExceeded,true);assert.equal(r.at,200);assert.equal(f.sent.length,1);assert.equal(f.active,0);await f.tick(1000);assert.equal(f.sent.length,1);assert.equal(f.timers,0);
});
test('retry receives only the remaining budget and cannot accept a late acknowledgement',async()=>{
 const f=fixture();f.api.limits.retryMs=[30,0];const r=f.track(f.api.send(f.body('same-key')));await f.tick(80);f.sent[0].finish('error');await f.tick(30);assert.equal(f.sent.length,2);assert.equal(f.sent[1].timeout,90);await f.tick(90);assert.equal(r.error.deadlineExceeded,true);assert.equal(r.at,200);f.sent[1].finish();assert.equal(r.value,undefined);assert.equal(f.sent[1].body.get('client_req_id'),'same-key');
});
test('expiry during backoff does not create another request',async()=>{
 const f=fixture();f.api.limits.retryMs=[150,0];const r=f.track(f.api.send(f.body('backoff')));await f.tick(100);f.sent[0].finish('error');await f.tick(100);assert.equal(r.error.deadlineExceeded,true);assert.equal(r.at,200);await f.tick(1000);assert.equal(f.sent.length,1);assert.equal(f.timers,0);
});
test('queued uploads expire promptly without waiting for or later stealing a request slot',async()=>{
 const f=fixture(),a=f.track(f.api.send(f.body('a'),{timeoutMs:1000})),b=f.track(f.api.send(f.body('b'),{timeoutMs:1000})),queued=f.track(f.api.send(f.body('queued')));await f.tick(200);assert.equal(f.sent.length,2);assert.equal(queued.error.deadlineExceeded,true);assert.equal(queued.at,200);f.sent.forEach(x=>x.finish());await f.tick(0);await Promise.all([a.done,b.done]);assert.equal(f.sent.length,2);assert.equal(f.timers,0);
});
test('queued and backoff cancellation clean up immediately and remain cancellation errors',async()=>{
 const f=fixture(),a=f.track(f.api.send(f.body('a'),{timeoutMs:1000})),b=f.track(f.api.send(f.body('b'),{timeoutMs:1000})),c=new AbortController(),q=f.track(f.api.send(f.body('q'),{signal:c.signal}));await f.tick(20);c.abort();await flush();assert.equal(q.error.cancelled,true);assert.equal(q.error.deadlineExceeded,undefined);assert.equal(q.at,20);f.sent.forEach(x=>x.finish());await f.tick(0);await Promise.all([a.done,b.done]);
 const stop=new AbortController(),r=f.track(f.api.send(f.body('retry'),{signal:stop.signal}));f.api.limits.retryMs=[150,0];await f.tick(20);f.sent.at(-1).finish('error');await f.tick(20);stop.abort();await flush();assert.equal(r.error.cancelled,true);assert.equal(f.timers,0);
});
test('progress can reset idle detection but never the total deadline',async()=>{
 const f=fixture();f.api.limits.idleMs=60;const r=f.track(f.api.send(f.body('progress')));await f.tick(0);for(let i=0;i<3;i++){await f.tick(50);f.sent[0].upload.onprogress({loaded:i+1,total:10,lengthComputable:true});}f.api.limits.totalMs=999999;await f.tick(50);assert.equal(r.error.deadlineExceeded,true);assert.equal(r.at,200);assert.equal(f.sent.length,1);
});
test('an acknowledgement at 199ms succeeds and removes every deadline/idle timer',async()=>{
 const f=fixture(),r=f.track(f.api.send(f.body('timely')));await f.tick(199);f.sent[0].finish('load',{ok:true,id:'timely'});await f.tick(1000);assert.equal(r.value.id,'timely');assert.equal(r.at,199);assert.equal(r.error,undefined);assert.equal(f.sent.length,1);assert.equal(f.timers,0);
});
test('zero, negative and non-finite deadlines cannot disable the total timeout',async()=>{
 const f=fixture();for(const timeoutMs of [0,-1,NaN,Infinity])await assert.rejects(f.api.send(f.body('invalid'),{timeoutMs}),e=>e.businessError);assert.equal(f.sent.length,0);assert.equal(f.timers,0);
});
test('acknowledged batch files survive another file deadline and manual continuation uses original keys',async()=>{
 const f=fixture(),items=[{file:f.body('a').get('file'),id:'a'},{file:f.body('b').get('file'),id:'b'}],upload=async item=>{if(item.done)return;await f.api.send(f.body(item.id));item.done=true;},r=f.track(f.api.files(items,upload));await f.tick(0);f.sent[0].finish();await f.tick(200);assert.equal(r.error.deadlineExceeded,true);assert.equal(items[0].done,true);assert.equal(items[1].done,undefined);const resumed=f.track(f.api.files(items,upload));await f.tick(0);assert.equal(f.sent.length,3);assert.equal(f.sent[2].body.get('client_req_id'),'b');f.sent[2].finish();await f.tick(0);assert.equal(resumed.error,undefined);assert.ok(items.every(x=>x.done));assert.equal(f.timers,0);
});
