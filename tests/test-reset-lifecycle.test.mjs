import test from 'node:test';import assert from 'node:assert/strict';
import {resetLifecycleFixture} from './test-reset-lifecycle-fixture.mjs';
const start=()=>({requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
const children=['ck_attendance_spans','ck_attendance_span_events','ck_attendance_span_workers','ck_dispatch_time_days','ck_dispatch_time_spans','ck_dispatch_time_links','ck_dispatch_time_actions','ck_dispatch_share_pairs','ck_dispatch_share_events'];
test('a real staging reset archives and clears returned attendance, dispatch labor and shared responsibility',async t=>{
 const f=await resetLifecycleFixture(t),before={};
 for(const name of children){before[name]=await f.rows('SELECT * FROM '+name+' ORDER BY rowid');assert.ok(before[name].length>0,name+' must contain actual API-created evidence');}
 const {run}=await f.okReset('start',start()),done=await f.complete(run);assert.ok(done.run.completedAt);
 for(const name of children){assert.deepEqual(await f.rows('SELECT * FROM '+name),[],name+' must not retain orphan business rows');assert.deepEqual(await f.rows('SELECT * FROM ck_backup_'+run.id+'_'+name+' ORDER BY rowid'),before[name],name+' backup must retain original historical bytes');}
 assert.equal((await f.query("SELECT location_name FROM v2_003_locations WHERE id='SYN-RESET-KEEP'").first()).location_name,'合成保留库位');
});
