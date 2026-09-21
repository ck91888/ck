import test from 'node:test';import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';import {ATTENDANCE_SCHEMA} from '../worker-v2/attendance.js';
import {resetGate,RESET_ID,RESET_DATABASE,RESET_TABLES} from '../worker-v2/staging-reset-entry.js';
const req=()=>new Request('https://ck-v2-api-sop-staging.ck91888.workers.dev/api/maintenance-20260921');
const at='2026-09-21T11:00:00.000Z';
async function setup(){const DB=database();for(const s of ATTENDANCE_SCHEMA)DB.raw.exec(s);
 DB.raw.exec("CREATE TABLE v2_schema_meta(key TEXT PRIMARY KEY,value TEXT,updated_at TEXT); INSERT INTO v2_schema_meta VALUES('schema_version','keep',''); INSERT INTO v2_003_locations(id,location_name) VALUES('KEEP','基础库位'); INSERT INTO v2_inbound_plans(id,customer,status) VALUES('TEST-IB','虚拟客户','pending'); INSERT INTO v2_idempotency_keys(idem_key,action,response_json,created_at) VALUES('old','test','{}','');");
 return {DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_TEST_RESET_RUN:RESET_ID,SOP_TEST_RESET_DATABASE:RESET_DATABASE};}
test('explicit staging host and database guards refuse every non-target before DB access',async()=>{
 const env={SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_TEST_RESET_RUN:RESET_ID,SOP_TEST_RESET_DATABASE:RESET_DATABASE};
 for(const changed of [{SOP_ENVIRONMENT:'production'},{SOP_UPGRADE_ENABLED:'false'},{SOP_TEST_RESET_RUN:''},{SOP_TEST_RESET_DATABASE:'production'}])assert.equal(await resetGate(req(),{...env,...changed},at),null);
 assert.equal(await resetGate(new Request('https://production.invalid/api'),env,at),null);
});
test('bounded reset archives data, preserves configuration and never clears a later upload',async()=>{
 const env=await setup();let result;
 for(let i=0;i<20;i++){result=await (await resetGate(req(),env,at)).json();if(!result.maintenance)break;}
 assert.ok(result.run.completed_at);assert.equal(result.tables_total,RESET_TABLES.length);assert.equal(result.tables_done,RESET_TABLES.length);assert.ok(result.remaining.every(x=>x.count===0));assert.equal(result.archived_rows,2);
 assert.equal(env.DB.raw.prepare('SELECT customer FROM ck_archive_20260921_v2_inbound_plans').get().customer,'虚拟客户');
 assert.equal(env.DB.raw.prepare('SELECT count(*) n FROM v2_003_locations').get().n,1);assert.equal(env.DB.raw.prepare('SELECT value FROM v2_schema_meta').get().value,'keep');
 env.DB.raw.exec("INSERT INTO v2_inbound_plans(id,customer,status) VALUES('AFTER','用户新上传','pending')");
 await resetGate(req(),env,at);assert.equal(env.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
 assert.equal(await resetGate(new Request('https://ck-v2-api-sop-staging.ck91888.workers.dev/api'),env,'2026-09-22T11:00:00Z'),null);
});
test('failed delete rolls back archive and marker, allowing a safe retry',async()=>{
 const env=await setup();env.DB.raw.exec("CREATE TRIGGER test_fail BEFORE DELETE ON v2_inbound_plans BEGIN SELECT RAISE(ABORT,'fixture failure'); END");
 await assert.rejects(async()=>{for(let i=0;i<20;i++)await resetGate(req(),env,at);},/fixture failure/);
 assert.equal(env.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);
 assert.equal(env.DB.raw.prepare("SELECT count(*) n FROM ck_staging_reset_tables WHERE table_name='v2_inbound_plans'").get().n,0);
 assert.equal(env.DB.raw.prepare("SELECT count(*) n FROM sqlite_master WHERE name='ck_archive_20260921_v2_inbound_plans'").get().n,0);
 env.DB.raw.exec('DROP TRIGGER test_fail');let r;for(let i=0;i<20;i++){r=await(await resetGate(req(),env,at)).json();if(!r.maintenance)break;}assert.ok(r.run.completed_at);
});
test('expired authorization leaves original data intact',async()=>{const env=await setup();const r=await resetGate(req(),env,'2026-09-22T00:00:00Z');assert.equal(r.status,503);assert.equal(env.DB.raw.prepare('SELECT count(*) n FROM v2_inbound_plans').get().n,1);});
