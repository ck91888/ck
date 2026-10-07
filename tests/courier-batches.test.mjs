import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import {handleCourier} from '../worker-v2/courier.js';import {digest} from '../worker-v2/access-control.js';
async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('local-courier-fixture-only')};let office='',field='';
 const request=async(action,b={},scope='office')=>{const res=await worker.fetch(new Request('https://fixture.local/'+(scope==='field'?'001/':'')+'api',{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://fixture.local',Cookie:scope==='office'?office:field},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);const r=await res.json();if(action==='sop_login'&&r.ok){if(scope==='office')office=res.headers.get('set-cookie').split(';')[0];else field=res.headers.get('set-cookie').split(';')[0];}return r;};
 const ok=async(a,b={},scope)=>{const r=await request(a,b,scope);assert.equal(r.ok,true,r.error);return r;};
 await ok('sop_login',{sop_key:'local-courier-fixture-only'});const people=[];
 for(let i=0;i<4;i++){const p=(await ok('sop_attendance_employee_register',{name:'隔离人员 '+i,employeeNo:'COURIER-'+i,department:'direct_ship'})).person;await ok('sop_attendance_checkin',{badge:p.badgeId});people.push({id:p.badgeId,name:p.name,person_id:p.id});}
 const staff=people.map(({id,name})=>({id,name}));await ok('sop_login',{badge:staff[0].id},'field');
 const start=(indices=[0,1],extra={},scope='field')=>ok('sop_courier_receive',{operation:'start',workers:indices.map(i=>staff[i]),lead_id:staff[indices[0]].id,...extra},scope);
 const scan=(batch,number='301000000101',owner='8-1',badge=staff[0].id,scope='field')=>request('sop_courier_receive',{batch_id:batch.id,tracking_no:number,owner,scanner_badge:badge},scope);
 const get=async b=>(await ok('sop_courier_detail',{batch_id:b.id})).batch;
 const finish=(b,extra={},scope='field')=>ok('sop_courier_receive',{operation:'finish',batch_id:b.id,revision:b.revision,...extra},scope);
 return {DB,env,request,ok,people,staff,start,scan,get,finish};
}
test('authenticated dispatch requires valid attended badges, records a whole crew and shares canonical server counts',async()=>{
 const f=await fixture();for(const workers of [[],[{id:'TEST-BYPASS',name:'伪造'}],[{...f.staff[0],name:'伪造姓名'}],[f.staff[0],f.staff[0]]])assert.equal((await f.request('sop_courier_receive',{operation:'start',workers,lead_id:workers[0]?.id},'field')).ok,false);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batches').get().n,0);
 const {batch:b}=await f.start();assert.match(b.batch_no,/^\d{4}-\d\d-\d\d-01$/);assert.equal(b.workers.length,2);assert.equal(b.total,0);
 const a=await f.scan(b);assert.equal(a.ok,true,a.error);const second=await f.scan(b,'301000000102','8-2',f.staff[1].id);assert.equal(second.ok,true,second.error);const d=await f.get(b);assert.equal(d.total,2);assert.deepEqual(d.counts,{'8-1':1,'8-2':1});assert.equal(a.item.batch_id,b.id);
 const multi=await f.ok('sop_courier_config',{operation:'batches'},'field');assert.equal(multi.items[0].id,b.id);
 await f.ok('sop_login',{badge:f.staff[1].id},'field');assert.equal((await f.ok('sop_courier_config',{operation:'batches'},'field')).items[0].id,b.id);assert.equal((await f.scan(b,'301000000103','8-1',f.staff[1].id)).ok,true);
 const conflict=await f.request('sop_courier_receive',{operation:'start',workers:[f.staff[1]],lead_id:f.staff[1].id},'field');assert.equal(conflict.ok,false);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batches').get().n,1);
});
test('different batches isolate counts; duplicate numbers retain original batch and historical receipts never gain a batch',async()=>{
 const f=await fixture(),a=(await f.start()).batch,b=(await f.start([2,3],{},'office')).batch;assert.notEqual(a.id,b.id);assert.match(b.batch_no,/-02$/);assert.equal((await f.scan(a)).ok,true);
 const repeat=await f.scan(b,'301000000101','8-4',f.staff[2].id,'office');assert.equal(repeat.duplicate,true);assert.equal(repeat.item.batch_id,a.id);assert.equal(repeat.item.owner,'8-1');assert.equal((await f.get(b)).total,0);
 f.DB.raw.exec("INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES('HISTORY','301000000109','unknown','2020-01-01T01:00:00Z','OLD','历史扫描人','OLD','历史登录人')");
 const old=await f.scan(b,'301000000109','8-2',f.staff[2].id,'office');assert.equal(old.ok,true,old.error);assert.equal(old.duplicate,true);assert.equal(old.item.batch_id,null);assert.equal(old.item.scanner_name,'历史扫描人');assert.equal((await f.get(b)).total,0);
 const search=await f.ok('sop_courier_list',{keyword:'301000000101'});assert.equal(search.items[0].batch_id,a.id);assert.equal((await f.ok('sop_courier_list',{mode:'batches',keyword:'301000000101'})).items[0].id,a.id);assert.equal((await f.ok('sop_courier_list',{history:true,keyword:'301000000109'})).total,1);
});
test('double click and concurrent request replay create one batch; collisions cannot change its crew',async()=>{
 const f=await fixture(),payload={client_req_id:crypto.randomUUID()};const results=await Promise.all([f.start([0,1],payload),f.start([0,1],payload)]);assert.equal(results[0].batch.id,results[1].batch.id);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batches').get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers').get().n,2);
 const bad=await f.request('sop_courier_receive',{operation:'start',workers:[f.staff[2]],lead_id:f.staff[2].id,...payload},'field');assert.equal(bad.ok,false);
 const done=await f.finish(results[0].batch);assert.equal((await f.start([0,1],payload)).batch.status,'completed');await f.ok('sop_attendance_checkout',{badge:f.staff[0].id});assert.equal((await f.request('sop_courier_receive',{operation:'start',workers:f.staff.slice(0,2),lead_id:f.staff[0].id,...payload},'field')).ok,false);assert.equal(done.batch.status,'completed');
});
test('parallel same tracking scans have one immutable receipt, link and event; replay after lost response counts once',async()=>{
 const f=await fixture(),b=(await f.start()).batch,r=await Promise.all([f.scan(b),f.scan(b,'301000000101','8-4',f.staff[1].id)]);assert.equal(r.every(x=>x.ok),true,JSON.stringify(r));assert.equal(r.filter(x=>!x.duplicate).length,1);assert.equal(r[0].item.id,r[1].item.id);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_events').get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batch_items').get().n,1);assert.equal((await f.scan(b)).duplicate,true);assert.equal((await f.get(b)).total,1);
});
test('database failure rolls back receipt, batch link and event and does not increase any counts',async()=>{
 const f=await fixture(),b=(await f.start()).batch;const original=f.DB.batch.bind(f.DB);f.DB.batch=async statements=>{if(statements.some(x=>x.sql.includes('INSERT INTO ck_courier_events')))throw Error('fixture connection failure');return original(statements);};assert.equal((await f.scan(b)).ok,false);f.DB.batch=original;assert.equal((await f.get(b)).total,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_receipts').get().n,0);assert.equal((await f.scan(b)).ok,true);
});
test('crew changes preserve remaining segments, leaving everyone keeps batch open; only explicit finish closes the group',async()=>{
 const f=await fixture(),b=(await f.start()).batch;const segment=f.DB.raw.prepare('SELECT id FROM v2_ops_job_workers WHERE job_id=? AND worker_id=?').get(b.id,f.staff[0].id).id;
 const changed=await f.ok('sop_courier_receive',{operation:'people',batch_id:b.id,revision:b.revision,workers:[f.staff[0],f.staff[2]],lead_id:f.staff[0].id},'field');assert.equal(changed.batch.workers.length,2);assert.equal(f.DB.raw.prepare("SELECT id FROM v2_ops_job_workers WHERE job_id=? AND worker_id=? AND left_at=''").get(b.id,f.staff[0].id).id,segment);assert.equal((await f.scan(b,'301000000102','8-1',f.staff[1].id)).ok,false);
 const payload={operation:'people',batch_id:b.id,revision:changed.batch.revision,workers:[],lead_id:'',client_req_id:crypto.randomUUID()},empty=await f.ok('sop_courier_receive',payload,'office');assert.equal(empty.batch.status,'awaiting_close');assert.equal(empty.batch.workers.length,0);assert.equal((await f.ok('sop_courier_config',{operation:'batches'},'office')).items[0].id,b.id);assert.equal((await f.ok('sop_courier_receive',payload,'office')).batch.id,b.id);
 const final=await f.finish(empty.batch,{},'office');assert.equal(final.batch.status,'completed');assert.equal((await f.scan(b)).ok,false);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(b.id).n,0);
});
test('completion and scan races serialize without phantom success; stale people changes cannot reopen the batch',async()=>{
 const f=await fixture(),b=(await f.start()).batch,original=f.DB.batch.bind(f.DB);let injected=false;
 f.DB.batch=async statements=>{if(!injected&&statements.some(x=>x.sql.includes('INSERT OR IGNORE INTO ck_courier_receipts'))){injected=true;await f.finish(b,{},'office');}return original(statements);};const r=await f.scan(b);assert.equal(injected,true);assert.equal(r.ok,false);f.DB.batch=original;assert.equal((await f.get(b)).total,0);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_receipts').get().n,0);
 assert.equal((await f.request('sop_courier_receive',{operation:'people',batch_id:b.id,revision:b.revision,workers:[f.staff[0]],lead_id:f.staff[0].id},'office')).ok,false);assert.equal((await f.finish(b,{},'office')).already_completed,true);
 const next=(await f.start()).batch;assert.equal((await f.scan(next,'301000000101')).ok,true);assert.equal((await f.get(next)).total,1);
});
test('checkout and removal racing with receipt commit reject the stale scanner',async()=>{
 for(const mutation of ['checkout','remove']){const f=await fixture(),b=(await f.start()).batch,original=f.DB.batch.bind(f.DB);let once=false;f.DB.batch=async statements=>{if(!once&&statements.some(x=>x.sql.includes('INSERT OR IGNORE INTO ck_courier_receipts'))){once=true;if(mutation==='checkout')await f.ok('sop_attendance_checkout',{badge:f.staff[1].id});else await f.ok('sop_courier_receive',{operation:'people',batch_id:b.id,revision:b.revision,workers:[f.staff[0]],lead_id:f.staff[0].id},'office');}return original(statements);};assert.equal((await f.scan(b,'301000000101','8-1',f.staff[1].id)).ok,false);f.DB.batch=original;assert.equal((await f.get(b)).total,0);}
});
test('cross Korean midnight continues original batch while new dispatch uses new date; 99 advances to 100',async()=>{
 const f=await fixture(),b=(await f.start()).batch;const now=new Date(),day=new Date(now.getTime()+9*3600000).toISOString().slice(0,10),oldDay=new Date(Date.parse(day+'T00:00:00+09:00')-86400000).toISOString().slice(0,10),oldTime=oldDay+'T10:00:00Z';
 f.DB.raw.prepare('UPDATE ck_courier_batches SET day=?,batch_no=? WHERE id=?').run(oldDay,oldDay+'-01',b.id);f.DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(oldTime,b.id);for(const p of f.staff.slice(0,2))f.DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=? WHERE worker_id=?').run(oldDay,oldTime,p.id);
 // Verified office session continues a crew already in progress; field session expiry is unchanged.
 const r=await f.scan(b,'301000000101','8-1',f.staff[0].id,'office');assert.equal(r.ok,true,r.error);assert.equal(r.item.batch_no,oldDay+'-01');
 f.DB.raw.prepare("INSERT INTO ck_courier_batches VALUES('SEQUENCE-99',?,99,?,?)").run(day,day+'-99',now.toISOString());const next=(await f.start([2,3],{},'office')).batch;assert.equal(next.batch_no,day+'-100');assert.equal(next.day,day);
});
test('field sessions keep original dispatch visibility and cannot manage another batch; reads remain gated',async()=>{
 const f=await fixture(),a=(await f.start()).batch,b=(await f.start([2,3],{},'office')).batch;assert.equal((await f.scan(b,'301000000101','8-1',f.staff[2].id)).ok,false);assert.equal((await f.request('sop_courier_receive',{operation:'finish',batch_id:b.id,revision:b.revision},'field')).ok,false);assert.equal((await f.ok('sop_courier_config',{operation:'batches'},'field')).items.length,1);assert.equal((await f.ok('sop_dispatch_list',{},'field')).items[0].job_type,'courier_receiving');
 await assert.rejects(handleCourier({action:'sop_courier_receive',operation:'start'},{...f.env,SOP_REQUEST_USER:{id:'R',name:'只读',role:'reviewer'}},()=>{}),/派审/);assert.equal((await f.get(a)).status,'working');
});
test('concurrent finish retry closes crew once, keeps completion timestamp and increments revision once',async()=>{
 const f=await fixture(),b=(await f.start()).batch,payload={client_req_id:crypto.randomUUID()},r=await Promise.all([f.finish(b,payload),f.finish(b,payload)]);assert.equal(r[0].batch.finished_at,r[1].batch.finished_at);assert.equal((await f.get(b)).revision,b.revision+1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batch_requests WHERE kind=?').get('finish').n,1);assert.equal((await f.finish(b,payload)).batch.finished_at,r[0].batch.finished_at);
});
test('simultaneous independent crews get distinct daily numbers; failed start consumes no sequence or worker segments',async()=>{
 const f=await fixture(),r=await Promise.all([f.start([0,1]),f.start([2,3],{},'office')]);assert.equal(new Set(r.map(x=>x.batch.batch_no)).size,2);assert.deepEqual(r.map(x=>x.batch.sequence).sort(),[1,2]);await f.finish(r[0].batch,{},'office');await f.finish(r[1].batch,{},'office');
 const original=f.DB.batch.bind(f.DB);let injected=false;f.DB.batch=async statements=>{if(!injected&&statements.some(x=>x.sql.includes('INSERT INTO ck_courier_batches'))){injected=true;return original([...statements,f.DB.prepare('SELECT * FROM missing_fixture_table')]);}return original(statements);};assert.equal((await f.request('sop_courier_receive',{operation:'start',workers:f.staff.slice(0,2),lead_id:f.staff[0].id},'field')).ok,false);f.DB.batch=original;assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batches').get().n,2);assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,0);assert.equal((await f.start()).batch.sequence,3);
});
test('off-duty, resting, disabled and not-yet-attended badges cannot create a batch',async()=>{
 for(const state of ['signed_out','resting','disabled','no_attendance']){const f=await fixture();if(state==='signed_out')await f.ok('sop_attendance_checkout',{badge:f.staff[2].id});if(state==='resting')await f.ok('sop_attendance_break_start',{badge:f.staff[2].id});if(state==='disabled')f.DB.raw.prepare('UPDATE ck_attendance_people SET enabled=0 WHERE id=?').run(f.people[2].person_id);if(state==='no_attendance')f.DB.raw.prepare('DELETE FROM ck_attendance_days WHERE worker_id=?').run(f.staff[2].id);assert.equal((await f.request('sop_courier_receive',{operation:'start',workers:[f.staff[2]],lead_id:f.staff[2].id},'field')).ok,false,state);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM ck_courier_batches').get().n,0);}
});
