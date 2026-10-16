import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';

async function fixture(t){const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());return f;}
const need=(f,department)=>f.ok('office','sop_need_create',{department,source_type:'inventory',supply_chain_no:'SYNTHETIC-DEPARTMENT-'+crypto.randomUUID(),customer:'合成部门验证',title:'合成库存翻新装箱',instructions:'合成入口与部门验证',planned_quantity:2,planned_unit:'箱'});
const dispatch=(f,n,department,w)=>f.call('A','sop_task_dispatch',{department,need_id:n.id,title:'合成库存翻新装箱',job_type:'bulk_op',estimated_minutes:30,workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId});
const snapshot=f=>Object.fromEntries(['sop_records','sop_events','v2_ops_jobs','v2_ops_job_workers'].map(table=>[table,f.DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));

test('bulk inventory need dispatch remains valid with one checked-in worker and original ownership',async t=>{
 const f=await fixture(t),n=await need(f,'bulk'),w=await f.person(),r=await dispatch(f,n,'bulk',w);assert.equal(r.ok,true,r.error);
 const resolved=await f.ok('A','sop_field_resolve',{code:n.id});assert.equal(resolved.need.department,'bulk');assert.equal(resolved.task.owner_id,f.people.A.id);assert.equal(resolved.segments.length,1);assert.equal(resolved.task.status,'working');
});
for(const department of ['direct_ship','import'])test('authorized '+department+' need stays readable but bulk dispatch rejects with the actual department and no business writes',async t=>{
 const f=await fixture(t),n=await need(f,department),w=await f.person(),resolved=await f.ok('A','sop_field_resolve',{code:n.id});assert.equal(resolved.need.department,department);
 const before=snapshot(f),r=await dispatch(f,n,department,w);assert.equal(r.ok,false);assert.ok(r.error.includes({direct_ship:'代发',import:'进口'}[department]));assert.match(r.error,/大货作业入口不能派工/);assert.deepEqual(snapshot(f),before);
});
test('forging bulk on a non-bulk need cannot relabel the plan or start its clock',async t=>{
 const f=await fixture(t),n=await need(f,'direct_ship'),w=await f.person(),before=snapshot(f),r=await dispatch(f,n,'bulk',w);assert.equal(r.ok,false);assert.match(r.error,/部门不符/);assert.deepEqual(snapshot(f),before);
});
test('department disclosure still requires the original department authorization',async t=>{
 const f=await fixture(t),n=await need(f,'import'),w=await f.person();
 f.DB.raw.prepare("UPDATE ck_access_sessions SET departments=? WHERE user_id=? AND scope='field'").run(JSON.stringify(['bulk']),f.people.A.id);
 assert.equal((await f.call('A','sop_field_resolve',{code:n.id})).ok,false);const before=snapshot(f),r=await dispatch(f,n,'import',w);assert.equal(r.ok,false);assert.match(r.error,/部门权限/);assert.deepEqual(snapshot(f),before);
});
