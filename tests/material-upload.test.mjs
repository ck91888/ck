import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import test from 'node:test';
const source=readFileSync(new URL('../shared/material-upload.js',import.meta.url),'utf8');
const delay=ms=>new Promise(r=>setTimeout(r,ms));
function fixture(run){
 const sent=[],window={CKSession:{user:{id:'synthetic-office',scope:'office',role:'service',operation_context:{id:'synthetic-context'}}}},location=new URL('http://localhost/002/');let active=0,peak=0;
 class XHR{
  constructor(){this.upload={};this.headers={};this.opened=false;}
  open(method,url){this.method=method;this.url=url;this.opened=true;}
  setRequestHeader(k,v){this.headers[k]=v;}
  send(body){this.body=body;this.live=true;active++;peak=Math.max(peak,active);sent.push(this);Promise.resolve().then(()=>run(this,sent.length));}
  finish(kind='load',status=200,body={ok:true,id:'synthetic-attachment'}){if(!this.live)return;this.live=false;active--;this.status=status;this.responseText=typeof body==='string'?body:JSON.stringify(body);this['on'+kind]?.();}
  abort(){this.finish('abort');}
 }
 vm.runInNewContext(source,{window,location,URL,FormData,XMLHttpRequest:XHR,setTimeout,clearTimeout,Promise});
 Object.assign(window.CKMaterialUpload.limits,{retryMs:[1,1],idleMs:500,totalMs:1000});
 return {api:window.CKMaterialUpload,window,sent,get peak(){return peak;},get active(){return active;}};
}
function body(id='CGF-synthetic',type='cargo_draft',file=new File(['synthetic PDF'],'synthetic.pdf',{type:'application/pdf'})){
 const data=new FormData();for(const[k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:type,related_doc_id:'synthetic-target',client_req_id:id,file}))data.set(k,v);return data;
}
test('transient failures retry the same file, target, request and operation context',async()=>{
 const f=fixture((x,n)=>x.finish(n<3?'error':'load'));assert.equal((await f.api.send(body())).ok,true);assert.equal(f.sent.length,3);
 for(const x of f.sent){assert.equal(x.body.get('client_req_id'),'CGF-synthetic');assert.equal(x.body.get('related_doc_id'),'synthetic-target');assert.equal(await x.body.get('file').text(),'synthetic PDF');assert.equal(x.headers['X-CK-Operation-Context'],'synthetic-context');assert.equal(x.withCredentials,true);}
});
for(const status of [400,401,403,404,409,413,422])for(const content of [{ok:false,error:'synthetic denial'},'<html>denied</html>'])test('never retries business/auth denial '+status+' '+typeof content,async()=>{
 const f=fixture(x=>x.finish('load',status,content));await assert.rejects(f.api.send(body()),e=>e.status===status);assert.equal(f.sent.length,1);
});
test('bounded retries recover 503 and incomplete success acknowledgement',async()=>{
 const f=fixture((x,n)=>n===1?x.finish('load',503,'gateway'):n===2?x.finish('load',200,'incomplete'):x.finish());await f.api.send(body());assert.equal(f.sent.length,3);
});
test('permanent disconnect stops at three attempts and keeps original request usable',async()=>{
 let fail=true;const f=fixture(x=>x.finish(fail?'error':'load')),data=body();await assert.rejects(f.api.send(data),/连接中断/);assert.equal(f.sent.length,3);fail=false;await f.api.send(data);assert.equal(f.sent.length,4);assert.equal(f.sent[3].body.get('client_req_id'),f.sent[0].body.get('client_req_id'));
});
test('identity change before acknowledgement stops without retry',async()=>{
 const f=fixture(x=>{f.window.CKSession.user=null;x.finish();});await assert.rejects(f.api.send(body()),e=>e.identityChanged);assert.equal(f.sent.length,1);
});
test('idle timeout is bounded and abort cancellation never retries',async()=>{
 const f=fixture(()=>{});Object.assign(f.api.limits,{idleMs:5,attempts:2});await assert.rejects(f.api.send(body()),/没有进展/);assert.equal(f.sent.length,2);assert.equal(f.active,0);
 const g=fixture(()=>{}),c=new AbortController(),p=g.api.send(body(),{signal:c.signal});await delay(1);c.abort();await assert.rejects(p,e=>e.cancelled);assert.equal(g.sent.length,1);
});
test('global request concurrency remains two; cancellation while queued sends nothing',async()=>{
 const f=fixture(()=>{}),a=f.api.send(body('a')),b=f.api.send(body('b')),c=new AbortController(),third=f.api.send(body('c'),{signal:c.signal});await delay(1);assert.equal(f.sent.length,2);c.abort();f.sent[0].finish();f.sent[1].finish();await Promise.all([a,b]);await assert.rejects(third,e=>e.cancelled);assert.equal(f.peak,2);assert.equal(f.sent.length,2);
});
test('batch waits for in-flight outcomes before throwing and starts no extra files',async()=>{
 const f=fixture(()=>{}),items=Array.from({length:6},(_,i)=>({file:body().get('file'),id:i})),started=[];let finish,settled=false;
 const task=f.api.files(items,async item=>{started.push(item.id);if(item.id===0)throw Error('synthetic failure');await new Promise(r=>finish=r);},{concurrency:2}).finally(()=>settled=true);
 await delay(1);assert.deepEqual(started,[0,1]);assert.equal(settled,false);finish();await assert.rejects(task,/synthetic failure/);assert.deepEqual(started,[0,1]);
});
test('batch validates all pending files before any send, but keeps acknowledged placeholders',async()=>{
 const f=fixture(x=>x.finish()),valid={file:body().get('file')},invalid={file:new File(['x'],'invalid.exe')};let called=0;
 await assert.rejects(f.api.files([valid,invalid],async()=>called++),/20MB/);assert.equal(called,0);
 await f.api.files([{file:new File([],'restored.pdf'),uploaded:true},valid],async item=>{if(!item.uploaded)await f.api.send(body());});assert.equal(f.sent.length,1);
});
test('file type and size limits match the server, and plan writes cannot be retried',async()=>{
 const f=fixture(x=>x.finish());for(const name of ['a.pdf','a.XLS','a.xlsx','a.csv','a.JPG','a.jpeg','a.png','a.webp'])assert.doesNotThrow(()=>f.api.validate(new File(['x'],name)));
 for(const file of [new File([],'empty.pdf'),new File(['x'],'a.exe'),{name:'too-large.pdf',size:20*1024*1024+1}])assert.throws(()=>f.api.validate(file));assert.doesNotThrow(()=>f.api.validate({name:'limit.pdf',size:20*1024*1024}));
 const data=body();data.set('action','v2_inbound_create');await assert.rejects(f.api.send(data),/编号或目标缺失/);assert.equal(f.sent.length,0);
});
