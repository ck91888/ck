import test from 'node:test';import assert from 'node:assert/strict';import vm from 'node:vm';import {readFileSync} from 'node:fs';
const source=readFileSync(new URL('../shared/sop-dispatch-ui.js',import.meta.url),'utf8');
function preview(detail){
 const storage=()=>{const m=new Map();return {getItem:k=>m.get(k)||null,setItem:(k,v)=>m.set(k,String(v)),removeItem:k=>m.delete(k)};};
 const calls=[],opened=[],errors=[];let navigated=0;
 const context={sessionStorage:storage(),localStorage:storage(),api:async b=>{calls.push(b);return detail;},saveActiveJob:(...args)=>opened.push(args),clearActiveJob(){},goMyTask(){navigated++;},alert:m=>errors.push(m)};
 context.window=context;vm.runInNewContext(source,context);context.CKInstallDispatch();
 return {context,calls,opened,errors,get navigated(){return navigated;}};
}
const live=()=>({ok:true,can_manage_dispatch:true,job:{id:'JOB-FIXTURE',job_type:'unload',status:'working',related_doc_type:'field_feedback',related_doc_id:'FB-FIXTURE'},dispatch:{state:JSON.stringify({lead_id:'EMP-A',workers:[{id:'EMP-A',name:'Old lead'}],last_lead:{id:'EMP-A',name:'Old lead'}})},workers:[{worker_id:'EMP-A',worker_name:'Old lead',left_at:'2000-01-01'},{worker_id:'EMP-B',worker_name:'Current crew',left_at:''}]});
test('resume reads the live crew, restores unplanned finish routing and never calls join/start',async()=>{
 const p=preview(live());for(let i=0;i<2;i++)await p.context.CKOpenNativeJob({id:'JOB-FIXTURE'});
 assert.equal(p.context.getWorkerId(),'EMP-B');assert.equal(p.context.localStorage.getItem('v2_unplanned_fb_id'),'FB-FIXTURE');
 assert.deepEqual(p.calls.map(x=>x.action),['v2_ops_job_detail','v2_ops_job_detail']);assert.equal(p.navigated,2);
 assert.deepEqual(p.opened,[['JOB-FIXTURE',null],['JOB-FIXTURE',null]]);assert.deepEqual(p.errors,[]);
});
test('resume of a planned unload clears a stale unplanned finish marker',async()=>{
 const detail=live();detail.job.related_doc_type='inbound_plan';const p=preview(detail);
 p.context.localStorage.setItem('v2_unplanned_fb_id','OLD');await p.context.CKOpenNativeJob({id:'JOB-FIXTURE'});
 assert.equal(p.context.localStorage.getItem('v2_unplanned_fb_id'),null);assert.equal(p.navigated,1);
});
test('a stale card cannot resume a finished job or one the dispatcher no longer manages',async()=>{
 for(const changes of [{can_manage_dispatch:false},{job:{...live().job,status:'completed'}}]){
  const p=preview({...live(),...changes});await p.context.CKOpenNativeJob({id:'JOB-FIXTURE'});
  assert.equal(p.navigated,0);assert.equal(p.opened.length,0);assert.equal(p.errors.length,1);
 }
});

test('single-person and whole-crew rests remain viewable without reopening clocks or starting a new job',async()=>{
 for(const size of [1,3]){
  const d=live();const staff=Array.from({length:size},(_,i)=>({id:'REST-'+i,name:'Rest '+i}));
  d.dispatch.state=JSON.stringify({lead_id:staff[0].id,workers:staff});
  d.workers=staff.map(w=>({worker_id:w.id,worker_name:w.name,left_at:'2026-10-08T03:00:00Z',leave_reason:'attendance:rest'}));
  const p=preview(d);for(let click=0;click<2;click++)await p.context.CKOpenNativeJob({id:d.job.id});
  assert.equal(p.navigated,2);assert.equal(p.context.getWorkerId(),'REST-0');assert.deepEqual(p.errors,[]);
  assert.deepEqual(p.calls.map(c=>c.action),['v2_ops_job_detail','v2_ops_job_detail']);
  const reloaded=preview(d);await reloaded.context.CKOpenNativeJob({id:d.job.id});assert.equal(reloaded.navigated,1);
 }
});
test('old dispatch with no crew snapshot can use its actual historical segment only after authorization',async()=>{
 const d=live();d.dispatch.state=JSON.stringify({lead_id:'EMP-A'});d.workers=d.workers.filter(w=>w.left_at);
 const p=preview(d);await p.context.CKOpenNativeJob({id:d.job.id});assert.equal(p.context.getWorkerId(),'EMP-A');assert.equal(p.navigated,1);
 for(const changed of [{can_manage_dispatch:false},{job:{...d.job,status:'completed'}},{job:{...d.job,status:'cancelled'}}]){
  const blocked=preview({...d,...changed});await blocked.context.CKOpenNativeJob({id:d.job.id});assert.equal(blocked.navigated,0);assert.equal(blocked.opened.length,0);
 }
});
