import {INBOUND_CREATE_RECEIPT_SQL} from '../worker-v2/inbound-create-receipt.js';
import test from 'node:test';import assert from 'node:assert/strict';
import {ATTENDANCE_MANAGEMENT_SCHEMA} from '../worker-v2/attendance-management.js';
import {PERMANENT_BADGE_SCHEMA} from '../worker-v2/permanent-badges.js';
import {COURIER_BATCH_SCHEMA} from '../worker-v2/courier-batches.js';
import {COURIER_CLAIM_HISTORY_SQL,ensureCourierClaims,claimCourierStatements} from '../worker-v2/courier-claims.js';
import {database} from './d1-adapter.mjs';import {ATTENDANCE_SCHEMA} from '../worker-v2/attendance.js';
import {resetAction,TABLES,STAGING_HOST,STAGING_HOSTS,STAGING_DATABASE,enabled,activeReset,ensureResetSchema} from '../worker-v2/test-data-reset.js';
import entry from '../worker-v2/staging-entry.js';
import {ACCESS_SCHEMA} from '../worker-v2/access-control.js';
import {LOAD_SCHEMA} from '../worker-v2/outbound-load-trip.js';
import {CREW_BORROW_SCHEMA} from '../worker-v2/crew-borrow.js';
import {BULK_BATCH_ITEMS_TABLE_SQL} from '../worker-v2/bulk-work-batch.js';
import {BULK_LABOR_FINISH_TABLE_SQL} from '../worker-v2/bulk-batch-labor.js';
const manager={id:'test-manager',role:'manager'};
function setup(){const DB=database();for(const s of [INBOUND_CREATE_RECEIPT_SQL,BULK_BATCH_ITEMS_TABLE_SQL,BULK_LABOR_FINISH_TABLE_SQL,...COURIER_BATCH_SCHEMA,COURIER_CLAIM_HISTORY_SQL,...ATTENDANCE_SCHEMA,...ATTENDANCE_MANAGEMENT_SCHEMA,...PERMANENT_BADGE_SCHEMA,...ACCESS_SCHEMA,...LOAD_SCHEMA,...CREW_BORROW_SCHEMA])DB.raw.exec(s);
 DB.raw.exec("CREATE TABLE v2_schema_meta(key TEXT PRIMARY KEY,value TEXT); INSERT INTO v2_schema_meta VALUES('version','keep'); INSERT INTO v2_003_locations(id,location_name) VALUES('KEEP','location'); INSERT INTO v2_inbound_plans(id,customer,status) VALUES('TEST','fixture','pending'); INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES('old','test','{}','');");
 const prepare=DB.prepare;let queries=0;
 DB.prepare=sql=>{queries++;assert.ok(!/UNION/i.test(sql),'avoid D1 compound-select limit');return prepare(sql);};
 return {DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE,SOP_PUBLIC_TEST_ACCESS:'true',measure:()=>{const n=queries;queries=0;return n;}};
}
const input=()=>({requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
async function act(env,action,body={}){env.measure();const r=await resetAction(action,body,env,manager);assert.ok(env.measure()<50,'under D1 Free query budget');return r;}
const request=(path,method='GET',body={},origin='https://'+STAGING_HOST)=>new Request('https://'+STAGING_HOST+path,{method,headers:method==='POST'?{'Content-Type':'application/json','Origin':origin,'X-CK-Test-Reset':'1'}:{},body:method==='POST'?JSON.stringify(body):undefined});
async function existing(env,completed=''){await ensureResetSchema(env);const id='a'.repeat(32);env.DB.raw.prepare('INSERT INTO ck_test_resets(id,request_id,actor,started_at,completed_at,tables_json) VALUES(?,?,?,?,?,?)').run(id,'synthetic-existing-run',manager.id,'2026-11-03T00:00:00Z',completed,JSON.stringify(['v2_inbound_plans']));return id;}
const disabled=e=>e.status===503&&e.reset_disabled===true;
const domainRequest=(host,path,method='GET',body={},options={})=>new Request('https://'+host+path,{
 method,headers:method==='POST'?{'Content-Type':'application/json','Origin':'https://'+host,'X-CK-Test-Reset':'1',...options.headers}:options.headers,
 body:method==='POST'?JSON.stringify(body):undefined
});

test('production, wrong host, wrong database and disabled switch cannot expose reset',async()=>{
 const env=setup();for(const change of [{SOP_ENVIRONMENT:'production'},{SOP_TEST_RESET_DATABASE:'production'},{SOP_TEST_RESET_ENABLED:'false'}]){assert.equal(enabled(request('/'),{...env,...change}),false);assert.equal((await entry.fetch(request('/api/test-data/start','POST',input()),{...env,...change})).status,404);}
 assert.equal(enabled(new Request('https://other.workers.dev'),env),false);assert.equal(env.measure(),0);
});
test('courier history bootstrap upgrades an existing schema without changing legacy business data',async()=>{
 const DB=database(),db=DB.raw,env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true'};
 assert.equal(db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_courier_plan_item_history'").get().n,0);
 db.exec("INSERT INTO v2_inbound_plans(id,customer,status,remark) VALUES('LEGACY','legacy fixture','cancelled','original cancellation'); INSERT INTO v2_inbound_plan_lines(id,plan_id,unit_type,planned_qty,actual_qty) VALUES('LEGACY-L','LEGACY','courier',1,1); INSERT INTO ck_courier_plan_items VALUES('301000000202','LEGACY','LEGACY-L'); INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_name,file_key) VALUES('LEGACY-A','inbound_plan','LEGACY','original.pdf','original-key')");
 db.prepare('INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES(?,?,?,?,?,?,?,?)').run('LEGACY-R','301000000202','8-1','2026-10-11T01:00:00.000Z','SCANNER','Original scanner','ACTOR','Original actor');
 const tables=['v2_inbound_plans','v2_inbound_plan_lines','ck_courier_plan_items','ck_courier_receipts','v2_attachments'];
 const before=tables.map(table=>db.prepare('SELECT * FROM '+table).all());
 await ensureCourierClaims(env);await ensureCourierClaims(env);
 assert.deepEqual(tables.map(table=>db.prepare('SELECT * FROM '+table).all()),before);
 assert.equal(db.prepare('SELECT COUNT(*) n FROM ck_courier_plan_item_history').get().n,0,'bootstrap must not release or rewrite historical claims');
 assert.equal(db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE type='trigger' AND name='ck_courier_history_delete_guard'").get().n,1);
});
test('reset accepts exactly the two approved test hosts and rejects production and similar names',async()=>{
 const env=setup();assert.deepEqual(STAGING_HOSTS,[STAGING_HOST,'sop-test.ck91888.cn']);
 for(const host of STAGING_HOSTS)assert.equal(enabled(domainRequest(host,'/api/test-data/status'),env),true);
 for(const host of ['api.ck91888.cn','ck91888.cn','test.ck91888.cn','other.ck91888.cn','sop-test.ck91888.cn.evil.invalid','ck-v2-api-sop-staging.other.workers.dev']){
  assert.equal(enabled(domainRequest(host,'/api/test-data/status'),env),false);
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/status'),env)).status,404);
 }
 assert.equal(env.measure(),0);
});
test('custom test host still requires staging environment, upgrade/reset flags and exact database marker',async()=>{
 const env=setup();for(const change of [{SOP_ENVIRONMENT:'production'},{SOP_ENVIRONMENT:undefined},{SOP_UPGRADE_ENABLED:'false'},{SOP_TEST_RESET_ENABLED:'false'},{SOP_TEST_RESET_DATABASE:'production'},{SOP_TEST_RESET_DATABASE:undefined}]){
  assert.equal(enabled(domainRequest('sop-test.ck91888.cn','/api/test-data/status'),{...env,...change}),false);
  assert.equal((await entry.fetch(domainRequest('sop-test.ck91888.cn','/api/test-data/start','POST',input()),{...env,...change})).status,404);
 }
 assert.equal(env.measure(),0);
});
test('both test hosts reject unauthenticated reset status and execution before creating a run',async()=>{
 const env={...setup(),SOP_ACCESS_CONTROL:'true',SOP_PUBLIC_TEST_ACCESS:'false'};
 for(const host of STAGING_HOSTS){
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/status'),env)).status,403);
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/start','POST',input()),env)).status,403);
 }
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM sqlite_master WHERE name=?').get('ck_test_resets').n,0);
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
});
test('normal isolated administrator login permits status on each approved host without clearing records',async()=>{
 // Login runs the normal migration path; the maintenance-only UNION budget wrapper does not apply.
 const env={DB:database(),SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE,SOP_ACCESS_CONTROL:'true',SOP_PUBLIC_TEST_ACCESS:'false',SOP_USERS_JSON:JSON.stringify([{id:'fixture-domain-manager',name:'Fixture manager',role:'manager',key:'isolated-domain-fixture-only'}])};
 env.DB.raw.exec("INSERT INTO v2_inbound_plans(id,customer,status) VALUES('TEST','fixture','pending')");
 for(const host of STAGING_HOSTS){
  const login=await entry.fetch(domainRequest(host,'/api','POST',{action:'sop_login',sop_key:'isolated-domain-fixture-only'}),env);assert.equal(login.status,200,await login.clone().text());
  const cookie=login.headers.get('set-cookie').split(';')[0];
  const response=await entry.fetch(domainRequest(host,'/api/test-data/status','GET',{}, {headers:{Cookie:cookie}}),env);assert.equal(response.status,200);
  const status=await response.json();assert.equal(status.ok,true);assert.equal(status.run,null);assert.equal(status.counts.find(x=>x.table_name==='v2_inbound_plans').count,1);
 }
 assert.equal(env.DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_test_resets'").get().n,0);
});
test('custom domain keeps POST, header and per-origin checks including cross-origin requests between test hosts',async()=>{
 const env=setup();assert.equal((await entry.fetch(domainRequest('sop-test.ck91888.cn','/api/test-data/start'),env)).status,405);
 for(const host of STAGING_HOSTS){const other=STAGING_HOSTS.find(x=>x!==host);
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/start','POST',input(),{headers:{Origin:'https://'+other}}),env)).status,403);
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/start','POST',input(),{headers:{'X-CK-Test-Reset':'0'}}),env)).status,403);
  assert.equal((await entry.fetch(domainRequest(host,'/api/test-data/start','POST',input(),{headers:{'Content-Type':'text/plain'}}),env)).status,403);
 }
 assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM sqlite_master WHERE name=?').get('ck_test_resets').n,0);
});

test('manager authority and original HTTP checks remain while valid reset execution is unavailable',async()=>{
 const env=setup();await assert.rejects(resetAction('start',input(),env,{role:'viewer'}),e=>e.status===403);
 await assert.rejects(act(env,'start',input()),disabled);await assert.rejects(act(env,'step',{}),disabled);
 assert.equal((await entry.fetch(request('/api/test-data/start'),env)).status,405);
 assert.equal((await entry.fetch(request('/api/test-data/start','POST',input(),'https://evil.invalid'),env)).status,403);
 assert.equal(env.DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_test_resets'").get().n,0);
});
test('status remains read-only without creating reset schema and all records/settings remain',async()=>{
 const env=setup(),preview=await act(env,'status');assert.equal(preview.ok,true);assert.equal(preview.disabled,true);assert.equal(preview.run,null);assert.equal(preview.counts.find(r=>r.table_name==='v2_inbound_plans').count,1);
 await assert.rejects(act(env,'start',input()),disabled);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_idempotency_keys').get().n,1);assert.equal(env.DB.raw.prepare('SELECT value FROM v2_schema_meta').get().value,'keep');assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_003_locations').get().n,1);assert.equal(env.DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_test_resets' OR name LIKE 'ck_backup_%'").get().n,0);
});
test('active and completed historical runs cannot advance or delete newer records on either host',async()=>{
 for(const completed of ['', '2026-11-03T01:00:00Z']){
  const env=setup(),id=await existing(env,completed);env.DB.raw.exec('CREATE TABLE ck_backup_synthetic_keep AS SELECT * FROM v2_inbound_plans');
  const before=env.DB.raw.prepare('SELECT * FROM ck_test_resets').get();
  for(const host of STAGING_HOSTS)for(const action of ['start','step']){const r=await entry.fetch(domainRequest(host,'/api/test-data/'+action,'POST',action==='start'?input():{runId:id}),env);assert.equal(r.status,503);}
  assert.deepEqual(env.DB.raw.prepare('SELECT * FROM ck_test_resets').get(),before);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_test_reset_items').get().n,0);assert.equal(env.DB.raw.prepare('SELECT customer FROM ck_backup_synthetic_keep').get().customer,'fixture');assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
 }
});
test('disabled reset preserves batch membership and unique labor cutoff bytes',async()=>{
 const env=setup(),db=env.DB.raw;db.prepare('INSERT INTO ck_bulk_work_batch_items VALUES(?,?,?,?,?)').run('FIXTURE-JOB','FIXTURE-BATCH',0,'need','FIXTURE-NEED');db.prepare('INSERT INTO ck_bulk_labor_finishes VALUES(?,?,?,?,?)').run('FIXTURE-BATCH','FIXTURE-JOB','FIXTURE-NEED','2026-10-15T01:00:00Z','first_order_reviewed');const member=db.prepare('SELECT * FROM ck_bulk_work_batch_items').get(),cutoff=db.prepare('SELECT * FROM ck_bulk_labor_finishes').get();await assert.rejects(act(env,'start',input()),disabled);await assert.rejects(act(env,'step',{}),disabled);assert.deepEqual(db.prepare('SELECT * FROM ck_bulk_work_batch_items').get(),member);assert.deepEqual(db.prepare('SELECT * FROM ck_bulk_labor_finishes').get(),cutoff);
});
test('concurrent reset attempts cannot create a run, backup or deletion',async()=>{
 const env=setup(),out=await Promise.allSettled(['start','start','step','step'].map(action=>resetAction(action,input(),env,manager)));assert.ok(out.every(r=>r.status==='rejected'&&disabled(r.reason)));assert.equal(env.DB.raw.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='ck_test_resets' OR name LIKE 'ck_backup_%'").get().n,0);assert.equal(env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,1);
});
test('disabled reset preserves released courier snapshots and their protected plans',async()=>{
 const env=setup(),db=env.DB.raw,code='301000000201';
 db.exec("INSERT INTO v2_inbound_plans(id,customer,status) VALUES('OLD','cancelled fixture','cancelled'); INSERT INTO v2_inbound_plan_lines(id,plan_id,unit_type,planned_qty,actual_qty) VALUES('OLD-L','OLD','courier',1,1),('NEW-L','TEST','courier',1,0)");
 db.prepare('INSERT INTO ck_courier_plan_items VALUES(?,?,?)').run(code,'OLD','OLD-L');
 db.prepare('INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES(?,?,?,?,?,?,?,?)').run('CR-RESET',code,'8-1','2026-10-12T01:00:00.000Z','SCANNER','Fixture scanner','ACTOR','Fixture actor');
 await ensureCourierClaims(env);
 await env.DB.batch(claimCourierStatements({...env,SOP_REQUEST_USER:{id:manager.id,name:'Fixture manager'}},'TEST','NEW-L',code));
 const history=db.prepare('SELECT * FROM ck_courier_plan_item_history').get();
 assert.equal(JSON.parse(history.receipt_json).receipt_id,'CR-RESET');assert.equal(history.replacement_plan_id,'TEST');
 assert.throws(()=>db.prepare('DELETE FROM v2_inbound_plans WHERE id=?').run('OLD'),/不能删除/);
 const preview=await act(env,'status');assert.equal(preview.counts.find(x=>x.table_name==='ck_courier_plan_item_history').count,1);
 await assert.rejects(act(env,'start',input()),disabled);await assert.rejects(act(env,'step',{}),disabled);assert.deepEqual(db.prepare('SELECT * FROM ck_courier_plan_item_history').get(),history);assert.equal(db.prepare('SELECT COUNT(*) n FROM v2_inbound_plans').get().n,2);assert.equal(db.prepare("SELECT status FROM v2_inbound_plans WHERE id='OLD'").get().status,'cancelled');assert.equal(db.prepare("SELECT received_at FROM ck_courier_receipts WHERE id='CR-RESET'").get().received_at,JSON.parse(history.receipt_json).received_at);
});
