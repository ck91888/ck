import test from 'node:test';import assert from 'node:assert/strict';
import {resetLifecycleFixture} from './test-reset-lifecycle-fixture.mjs';
import {TABLES} from '../worker-v2/test-data-reset.js';
import fs from 'node:fs';
const start=()=>({requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
const children=['ck_attendance_spans','ck_attendance_span_events','ck_attendance_span_workers','ck_dispatch_time_days','ck_dispatch_time_spans','ck_dispatch_time_links','ck_dispatch_time_actions','ck_dispatch_share_pairs','ck_dispatch_share_events','ck_daily_dispatch_grants','ck_daily_dispatch_links','ck_daily_dispatch_events','ck_inbound_document_versions','ck_batch_material_state','ck_batch_material_events'];
test('a real staging reset archives and clears returned attendance, dispatch labor and shared responsibility',async t=>{
 const f=await resetLifecycleFixture(t),before={};
 for(const name of children){before[name]=await f.rows('SELECT * FROM '+name+' ORDER BY rowid');assert.ok(before[name].length>0,name+' must contain actual API-created evidence');}
 const {run}=await f.okReset('start',start()),done=await f.complete(run);assert.ok(done.run.completedAt);
 for(const name of children){assert.deepEqual(await f.rows('SELECT * FROM '+name),[],name+' must not retain orphan business rows');assert.deepEqual(await f.rows('SELECT * FROM ck_backup_'+run.id+'_'+name+' ORDER BY rowid'),before[name],name+' backup must retain original historical bytes');}
 assert.equal((await f.query("SELECT location_name FROM v2_003_locations WHERE id='SYN-RESET-KEEP'").first()).location_name,'合成保留库位');
 assert.equal(f.objects.size,1);
 assert.ok((await f.okReset('status')).counts.every(row=>row.count===0));t.diagnostic(JSON.stringify({archivedTables:done.run.total,archivedRows:done.run.archived,r2Objects:f.objects.size}));
});
test('every declared business table has an explicit reset or retained-control classification',()=>{
 const retained=new Set(['ck_access_attempts','ck_access_sessions','ck_attendance_return_config','ck_attendance_return_migration_guard','ck_dispatch_time_config','ck_office_operation_context','ck_test_reset_items','ck_test_resets','v2_003_locations','v2_schema_meta']),root=new URL('../worker-v2/',import.meta.url),unknown=[];
 for(const file of fs.readdirSync(root).filter(name=>name.endsWith('.js')))for(const [,name]of fs.readFileSync(new URL(file,root),'utf8').matchAll(/CREATE TABLE(?: IF NOT EXISTS)?\s+(\w+)/g))if(!TABLES.includes(name)&&!retained.has(name))unknown.push({file,name});
 assert.deepEqual(unknown,[],'new schema additions must be deliberately classified, never wildcard-deleted');assert.equal(new Set(TABLES).size,TABLES.length);
});
test('two resumptions upgrade an older active scope without replacing completed backups',async t=>{
 const f=await resetLifecycleFixture(t),{run}=await f.okReset('start',start()),row=await f.query('SELECT * FROM ck_test_resets WHERE id=?',run.id).first(),original=await f.rows('SELECT * FROM sop_events ORDER BY rowid');
 await f.query('UPDATE ck_test_resets SET tables_json=? WHERE id=?',JSON.stringify(JSON.parse(row.tables_json).filter(name=>!children.includes(name))),run.id).run();
 const archive='ck_backup_'+run.id+'_sop_events';await f.env.DB.batch([f.query('INSERT INTO ck_test_reset_items SELECT ?,?,?,COUNT(*) FROM sop_events',run.id,'sop_events',archive),f.query('CREATE TABLE '+archive+' AS SELECT * FROM sop_events'),f.query('DELETE FROM sop_events')]);
 await Promise.all([f.okReset('step',{runId:run.id}),f.okReset('step',{runId:run.id})]);const done=await f.complete(run);assert.ok(done.run.completedAt);
 assert.deepEqual(await f.rows('SELECT * FROM '+archive+' ORDER BY rowid'),original);for(const name of children){assert.deepEqual(await f.rows('SELECT * FROM '+name),[]);assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=? AND table_name=?',run.id,name).first()).n,1);}
});
test('a child deletion failure rolls back its backup and marker, then the same run resumes',async t=>{
 const f=await resetLifecycleFixture(t),before=await f.rows('SELECT * FROM ck_attendance_spans ORDER BY rowid');await f.query("CREATE TRIGGER synthetic_return_reset_failure BEFORE DELETE ON ck_attendance_spans BEGIN SELECT RAISE(ABORT,'synthetic child failure'); END").run();
 const {run}=await f.okReset('start',start());let failure;for(let i=0;i<50;i++){const r=await f.reset('step',{runId:run.id});if(!r.result.ok){failure=r;break;}}
 assert.ok(failure);assert.match(failure.result.error,/synthetic child failure/);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans ORDER BY rowid'),before);
 assert.equal((await f.query("SELECT COUNT(*) n FROM sqlite_master WHERE name=?",'ck_backup_'+run.id+'_ck_attendance_spans').first()).n,0);assert.equal((await f.query("SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=? AND table_name='ck_attendance_spans'",run.id).first()).n,0);
 await f.query('DROP TRIGGER synthetic_return_reset_failure').run();await f.complete(run);assert.deepEqual(await f.rows('SELECT * FROM ck_backup_'+run.id+'_ck_attendance_spans ORDER BY rowid'),before);
});
test('an unexpected row in an already archived new table is preserved and blocks completion',async t=>{
 const f=await resetLifecycleFixture(t),original=(await f.rows('SELECT * FROM ck_dispatch_share_pairs'))[0],{run}=await f.okReset('start',start());
 for(let i=0;i<50&&!await f.query("SELECT 1 FROM ck_test_reset_items WHERE run_id=? AND table_name='ck_dispatch_share_pairs'",run.id).first();i++)await f.okReset('step',{runId:run.id});
 const columns=Object.keys(original);await f.query('INSERT INTO ck_dispatch_share_pairs('+columns.join(',')+') VALUES('+columns.map(()=>'?').join(',')+')',...Object.values(original)).run();
 let failure;for(let i=0;i<50;i++){const r=await f.reset('step',{runId:run.id});if(!r.result.ok){failure=r;break;}}
 assert.equal(failure?.status,409);assert.match(failure.result.error,/新增记录/);assert.deepEqual(await f.rows('SELECT * FROM ck_dispatch_share_pairs'),[original]);assert.equal((await f.query('SELECT completed_at FROM ck_test_resets WHERE id=?',run.id).first()).completed_at,'');
 assert.deepEqual(await f.rows('SELECT * FROM ck_backup_'+run.id+'_ck_dispatch_share_pairs'),[original]);
});
test('completed replay leaves new attendance intact and cleared badges can start a fresh workday',async t=>{
 const f=await resetLifecycleFixture(t),body=start(),{run}=await f.okReset('start',body);await f.complete(run);
 assert.equal((await f.call('A','sop_identity')).ok,false,'cleared personnel must not retain field authority');assert.equal((await f.call('office','sop_identity')).ok,true);
 const fresh=(await f.ok('office','sop_attendance_employee_register',{employeeNo:'OWNED-A',name:'合成新一轮派审',department:'bulk'})).person,attendance=(await f.ok('office','sop_attendance_checkin',{badge:fresh.badgeId})).record;
 assert.ok((await f.rows('SELECT * FROM ck_attendance_spans WHERE attendance_id=?',attendance.id)).length);const before=await f.rows('SELECT * FROM ck_attendance_spans ORDER BY rowid');
 assert.equal((await f.okReset('start',body)).run.id,run.id);await f.okReset('step',{runId:run.id});assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans ORDER BY rowid'),before);
 await f.ok('A','sop_login',{badge:fresh.badgeId});await f.task('A','合成重置后实际新派审');assert.equal((await f.query('SELECT COUNT(*) n FROM ck_dispatch_time_spans').first()).n,1);assert.equal((await f.query('SELECT person_id FROM ck_dispatch_time_spans').first()).person_id,fresh.id);
});
test('an altered scope cannot include retained authorization tables',async t=>{
 const f=await resetLifecycleFixture(t),{run}=await f.okReset('start',start()),before=await f.rows('SELECT * FROM ck_access_sessions ORDER BY rowid');
 await f.query('UPDATE ck_test_resets SET tables_json=? WHERE id=?',JSON.stringify(['ck_access_sessions']),run.id).run();const response=await f.reset('step',{runId:run.id});
 assert.equal(response.status,409);assert.match(response.result.error,/Invalid maintenance scope/);assert.deepEqual(await f.rows('SELECT * FROM ck_access_sessions ORDER BY rowid'),before);assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=?',run.id).first()).n,0);
});
