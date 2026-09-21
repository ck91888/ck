import test from 'node:test';import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';import {ATTENDANCE_SCHEMA} from '../worker-v2/attendance.js';
import {resetAction,TABLES,STAGING_HOST,STAGING_DATABASE,enabled,activeReset} from '../worker-v2/test-data-reset.js';
import entry from '../worker-v2/staging-entry.js';
const manager={id:'test-manager',role:'manager'};
function setup(){const DB=database();for(const s of ATTENDANCE_SCHEMA)DB.raw.exec(s);
 DB.raw.exec("CREATE TABLE v2_schema_meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO v2_schema_meta VALUES('version','keep'); INSERT INTO v2_003_locations(id,location_name) VALUES('KEEP','location'); INSERT INTO v2_inbound_plans(id,customer,status) VALUES('TEST','fixture','pending'); INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES('old','test','{}','');");
 const prepare=DB.prepare;let queries=0;
 DB.prepare=sql=>{queries++;assert.ok(!/UNION/i.test(sql),'avoid D1 compound-select limit');return prepare(sql);};
 return {DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE,SOP_PUBLIC_TEST_ACCESS:'true',measure:()=>{const n=queries;queries=0;return n;}};
}
const input=()=>({requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
async function act(env,action,body={}){env.measure();const r=await resetAction(action,body,env,manager);assert.ok(env.measure()<50,'under D1 Free query budget');return r;}
async function complete(env,id){let r;for(let i=0;i<20;i++){r=await act(env,'step',{runId:id});if(r.run.completedAt)return r;}assert.fail('reset did not finish');}
const request=(path,method='GET',body={},origin='https://'+STAGING_HOST)=>new Request('https://'+STAGING_HOST+path,{method,headers:method==='POST'?{'Content-Type':'application/json','Origin':origin,'X-CK-Test-Reset':'1'}:{},body:method==='POST'?JSON.stringify(body):undefined});
test('production, wrong host, wrong database and disabled switch cannot expose reset',async()=>{
 const env=setup();for(const change of [{SOP_ENVIRONMENT:'production'},{SOP_TEST_RESET_DATABASE:'production'},{SOP_TEST_RESET_ENABLED:'false'}]){assert.equal(enabled(request('/'),{...env,...change}),false);assert.equal((await entry.fetch(request('/api/test-data/start','POST',input()),{...env,...change})).status,404);}
 assert.equal(enabled(new Request('https://other.workers.dev'),env),false);assert.equal(env.measure(),0);
});
test('manager permission, confirmation, POST and same-origin headers required',async()=>{
 const env=setup();await assert.rejects(resetAction('start',input(),env,{id:'viewer',role:'viewer'}),/仅负责人/);
 await assert.rejects(act(env,'start',{requestId:crypto.randomUUID()}),/确认/);
 assert.equal((await entry.fetch(request('/api/test-data/start'),env)).status,405);
 assert.equal((await entry.fetch(request('/api/test-data/start','POST',input(),'https://evil.invalid'),env)).status,403);
 const noHeader=new Request('https://'+STAGING_HOST+'/api/test-data/start',{method:'POST',headers:{'Content-Type':'application/json','Origin':'https://'+STAGING_HOST},body:JSON.stringify(input())});assert.equal((await entry.fetch(noHeader,env)).status,403);
 assert.equal(await activeReset(env),null);
});
test('preview is read-only; all fixtures archived and cleared, settings retained',async()=>{
 const env=setup();const preview=await act(env,'status');assert.equal(preview.counts.find(x=>x.table_name==='v2_inbound_plans').count,1);assert.equal(preview.run,null);
 const {run}=await act(env,'start',input());assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
 const done=await complete(env,run.id);assert.equal(done.run.total,TABLES.length);assert.equal(done.run.done,TABLES.length);assert.equal(done.run.archived,2);
 assert.equal(env.DB.raw.prepare(`SELECT customer FROM ck_backup_${run.id}_v2_inbound_plans`).get().customer,'fixture');
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_locations').get().n,1);assert.equal(env.DB.raw.prepare('SELECT value FROM v2_schema_meta').get().value,'keep');assert.equal((await act(env,'status')).counts.every(x=>x.count===0),true);
});
test('duplicate start and completed step never delete newly uploaded records; next explicit reset is independent',async()=>{
 const env=setup(),body=input();const first=await act(env,'start',body);const repeated=await act(env,'start',input());assert.equal(repeated.run.id,first.run.id);await complete(env,first.run.id);
 env.DB.raw.exec("INSERT INTO v2_inbound_plans(id,customer,status) VALUES('NEW','user upload','pending')");
 assert.equal((await act(env,'start',body)).run.id,first.run.id);await act(env,'step',{runId:first.run.id});assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
 const next=await act(env,'start',input());assert.notEqual(next.run.id,first.run.id);await complete(env,next.run.id);assert.equal(env.DB.raw.prepare(`SELECT customer FROM ck_backup_${next.run.id}_v2_inbound_plans`).get().customer,'user upload');
});
test('failed table deletion rolls back backup and marker, then resumes after failure resolved',async()=>{
 const env=setup();env.DB.raw.exec("CREATE TRIGGER fixture_failure BEFORE DELETE ON v2_inbound_plans BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
 const {run}=await act(env,'start',input());await assert.rejects(complete(env,run.id),/fixture failure/);
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);assert.equal(env.DB.raw.prepare("SELECT COUNT(*) n FROM ck_test_reset_items WHERE table_name='v2_inbound_plans'").get().n,0);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM sqlite_master WHERE name=?').get(`ck_backup_${run.id}_v2_inbound_plans`).n,0);
 env.DB.raw.exec('DROP TRIGGER fixture_failure');assert.ok((await complete(env,run.id)).run.completedAt);
});
test('active reset blocks ordinary business mutations and preserves unexpected new writes',async()=>{
 const env=setup();const {run}=await act(env,'start',input());const r=await entry.fetch(request('/api','POST',{action:'v2_inbound_plan_create',customer:'unwanted'}),env);assert.equal(r.status,503);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
 await act(env,'step',{runId:run.id});await act(env,'step',{runId:run.id});env.DB.raw.exec("INSERT INTO ck_attendance_people(id,badge_id,name,agency,kind,created_at) VALUES('unexpected','unexpected','late write','test','daily','')");
 await assert.rejects(complete(env,run.id),/新增记录/);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_attendance_people').get().n,1);assert.equal((await act(env,'status')).run.completedAt,'');
});
test('concurrent start and step requests share one run and archive each table once',async()=>{
 const env=setup();const starts=await Promise.all([resetAction('start',input(),env,manager),resetAction('start',input(),env,manager)]);assert.equal(starts[0].run.id,starts[1].run.id);
 const id=starts[0].run.id;await Promise.all([resetAction('step',{runId:id},env,manager),resetAction('step',{runId:id},env,manager)]);await complete(env,id);
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_test_resets').get().n,1);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_test_reset_items').get().n,TABLES.length);
});
