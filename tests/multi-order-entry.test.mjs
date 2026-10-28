import test from 'node:test';
import assert from 'node:assert/strict';
import {executionFixture} from './work-execution-fixture.mjs';

test('reopening a grouped plan after cancelling its batch offers unassigned work and retains the explicit historical task',async t=>{
 const f=await executionFixture(t),worker=await f.person(),needs=[];
 for(const department of ['direct_ship','import'])needs.push(await f.historical(department,{cargo_groups:{groups:[{range:'SYNTHETIC-'+department,box_count:1,process_names:['贴标']}]}}));
 const batch=await f.ok('A','sop_bulk_batch_create',{items:needs.map(n=>({source_type:'need',id:n.id,revision:1})),workers:[{id:worker.badgeId,name:worker.name}],lead_id:worker.badgeId});
 let detail=await f.ok('A','sop_bulk_batch_get',{id:batch.id});const originalTasks=detail.items.map(x=>x.id);
 for(const id of originalTasks){detail=await f.ok('A','sop_bulk_batch_get',{id:batch.id});await f.ok('A','sop_bulk_batch_cancel',{id:batch.id,revision:detail.batch.revision,job_id:id,reason:'Synthetic cancellation'});}
 const snapshot=()=>Object.fromEntries(['sop_records','sop_events','v2_ops_jobs','v2_ops_job_workers'].map(table=>[table,f.DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()])),before=snapshot();
 for(const n of needs){const r=await f.ok('A','sop_field_resolve',{code:n.id});assert.equal(r.task,null);assert.equal(r.tasks.length,1);assert.equal(r.tasks[0].status,'cancelled');assert.ok(r.need.cargo_groups.groups.every(g=>g.processes.every(p=>p.status==='pending'&&!p.task_id)));}
 const history=await f.ok('A','sop_field_resolve',{code:originalTasks[0]});assert.equal(history.task.id,originalTasks[0]);assert.equal(history.task.status,'cancelled');assert.deepEqual(snapshot(),before);
 const candidates=(await f.ok('A','sop_bulk_batch_candidates')).items;assert.ok(needs.every(n=>candidates.some(x=>x.id===n.id)));
 const next=await f.ok('A','sop_bulk_batch_create',{items:needs.map(n=>({source_type:'need',id:n.id,revision:f.record(n.id).revision})),workers:[{id:worker.badgeId,name:worker.name}],lead_id:worker.badgeId});assert.notEqual(next.id,batch.id);assert.ok(originalTasks.every(id=>f.record(id).status==='cancelled'));assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at=''").get().n,1);assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results').get().n,0);
});
