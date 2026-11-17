import test from 'node:test';
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
const ids=r=>r.items.map(x=>x.id).sort();
test('personal list uses authenticated ownership, excludes membership and shared-department work, and scopes inner plan list',async t=>{
 const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());const own=await f.task('A','合成本人任务'),other=await f.task('B','合成同部门他人任务',f.people.A);
 const list=await f.mine('A');assert.equal(list.viewer_id,f.people.A.id);assert.deepEqual(ids(list),[own.id]);assert.deepEqual(ids(await f.ok('A','sop_dispatch_list',{view:'mine',owner_id:f.people.B.id,worker_id:f.people.B.badgeId})),[own.id]);
 assert.deepEqual(ids(await f.ok('A','sop_list',{kind:'task',view:'mine'})),[own.id]);assert.ok((await f.ok('A','sop_dispatch_list')).items.some(x=>x.id===other.id),'explicit existing discovery remains available');assert.deepEqual(ids(await f.mine('B')),[other.id]);
 assert.equal((await f.call('A','sop_task_pause',{id:other.id,revision:1,reason:'合成越权验证'})).ok,false);
});
test('owner scans the checked-in recipient badge, transfers one original task with audit, and list/action ownership moves without clock changes',async t=>{
 const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());const task=await f.task('A','合成原单交接'),before=f.clocks(),r=f.record(task.id),unavailable=(await f.ok('office','sop_attendance_employee_register',{employeeNo:'OWNED-OFF',name:'合成未签到接手人',department:'bulk'})).person;
 assert.equal((await f.call('A','sop_owner_transfer',{id:task.id,revision:r.revision,target_badge:unavailable.badgeId,reason:'合成目标未签到'})).ok,false);assert.deepEqual(f.clocks(),before);assert.equal(f.record(task.id).owner_id,f.people.A.id);
 await f.ok('A','sop_owner_transfer',{id:task.id,revision:r.revision,target_badge:f.people.B.badgeId+'|无效姓名标签',reason:'合成扫码交接'});
 assert.deepEqual(ids(await f.mine('A')),[]);assert.deepEqual(ids(await f.mine('B')),[task.id]);assert.deepEqual(f.clocks(),before);const after=f.record(task.id);assert.equal(after.owner_id,f.people.B.id);assert.equal(after.owner,f.people.B.name);assert.equal(after.original_owner_id,f.people.A.id);assert.equal(after.owner_transfers.at(-1).from_id,f.people.A.id);
 const audit=f.DB.raw.prepare("SELECT * FROM sop_events WHERE record_id=? AND action='sop_owner_transfer'").get(task.id);assert.equal(audit.actor_id,f.people.A.id);
 assert.equal((await f.call('A','sop_task_pause',{id:task.id,revision:after.revision,reason:'合成旧负责人操作'})).ok,false);assert.equal((await f.call('A','sop_owner_transfer',{id:task.id,revision:r.revision,target_badge:f.people.A.badgeId,reason:'合成旧版本'})).ok,false);
 await f.ok('B','sop_task_pause',{id:task.id,revision:after.revision,reason:'合成新负责人可操作'});assert.equal(f.record(task.id).status,'paused');
});
test('off-duty emergency claim remains explicit and is absent from the personal homepage until ownership changes',async t=>{
 const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());const task=await f.task('A','合成离岗紧急接班');await f.ok('office','sop_attendance_checkout',{id:f.people.A.record.id});const clocks=f.clocks();
 assert.deepEqual(ids(await f.mine('B')),[]);const option=await f.ok('B','sop_owner_transfer_options',{id:task.id});assert.equal(option.can_claim,true);const resolved=await f.ok('B','sop_field_resolve',{code:task.id});assert.equal(resolved.task.id,task.id);
 assert.equal((await f.call('B','sop_owner_transfer',{id:task.id,revision:option.revision,target_badge:f.people.A.badgeId,reason:'合成不能代他人接班'})).ok,false);
 await f.ok('B','sop_owner_transfer',{id:task.id,revision:option.revision,target_badge:f.people.B.badgeId,reason:'合成本人明确紧急接班'});assert.deepEqual(ids(await f.mine('B')),[task.id]);assert.deepEqual(f.clocks(),clocks);
});
test('batch ownership is filtered before cards are grouped and explicit transfer moves every unfinished order with one unchanged root clock',async t=>{
 const f=await ownedHomeFixture();t.after(()=>f.DB.raw.close());const needs=[await f.need('合成批次一'),await f.need('合成批次二')],w=await f.person(),created=await f.ok('A','sop_bulk_batch_create',{items:needs.map(n=>({source_type:'need',id:n.id,revision:1})),workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId});
 const batch=await f.ok('A','sop_bulk_batch_get',{id:created.batch_id}),children=batch.items.map(x=>x.id).sort(),clocks=f.clocks();assert.equal(clocks.filter(x=>!x.left_at).length,1);assert.deepEqual(ids(await f.mine('A')),children);assert.deepEqual(ids(await f.mine('B')),[]);
 const child=batch.items[0];await f.ok('A','sop_owner_transfer',{id:child.id,revision:child.revision,target_badge:f.people.B.badgeId,reason:'合成整批扫码交接'});assert.deepEqual(ids(await f.mine('A')),[]);assert.deepEqual(ids(await f.mine('B')),children);assert.equal(f.record(created.batch_id).owner_id,f.people.B.id);assert.deepEqual(f.clocks(),clocks);
 assert.equal((await f.call('A','sop_bulk_batch_get',{id:created.batch_id})).ok,false);assert.equal((await f.ok('B','sop_bulk_batch_get',{id:created.batch_id})).items.length,2);
});
