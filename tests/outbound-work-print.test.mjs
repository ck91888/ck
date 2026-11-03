// Historical source shapes in synthetic databases; no deprecated creation path
// is reopened and no production/remote database is used.
import test from 'node:test';import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
async function fixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
 const f=await ownedHomeFixture();let native;
 t.after(async()=>{await native?.close();f.DB.raw.close();});
 f.rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 f.run=(sql,...args)=>f.env.DB.prepare(sql).bind(...args).run();
 f.add=async(type='outbound',status=type==='outbound'?'pending_issue':'pending')=>{
  const id='SYN-SOURCE-'+crypto.randomUUID(),table=type==='outbound'?'v2_outbound_orders':'v2_inbound_plans';
  const n=await f.ok('office','sop_need_create',{source_type:'inventory',supply_chain_no:'SYN-HISTORY-'+id,customer:'合成历史打印客户',department:'bulk',title:'合成来源作业单',instructions:'保留合成作业资料与版本'});
  await f.run(`INSERT INTO ${table}(id,display_no,customer,biz_class,status) VALUES(?,?,?,?,?)`,id,'SYN-NO-'+id,'合成历史打印客户','bulk',status);
  const row=(await f.rows('SELECT state FROM sop_records WHERE id=?',n.id))[0];
  await f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify({...JSON.parse(row.state),source_type:type,source_id:id}),n.id);
  return {id:n.id,source_id:id,source_type:type,table};
 };
 f.ready=async()=>{if(process.env.CK_OUTBOUND_PRINT_NATIVE_D1==='true'&&!native){native=await localD1(f.DB.raw);f.env.DB=native;}};
 f.body=async needs=>{const items=[];for(const n of needs){const x=(await f.ok('office','sop_get',{id:n.id})).record;items.push({id:x.id,revision:x.revision,document_key:x.print_status.document_key});}return {items,client_req_id:crypto.randomUUID()};};
 f.print=(body,scope='office')=>f.call(scope,'sop_need_print_request',body);
 f.events=()=>f.rows("SELECT * FROM sop_events WHERE action='sop_need_print_request' ORDER BY request_id");
 f.snapshot=async()=>JSON.stringify(await Promise.all(['sop_records','sop_document_numbers','v2_outbound_orders','v2_outbound_order_lines','v2_inbound_plans','v2_inbound_plan_lines','v2_attachments','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results'].map(table=>f.rows('SELECT * FROM '+table+' ORDER BY rowid'))));
 return f;
}
test('historical outbound origin prints on the actual old schema without an is_deleted column',async t=>{
 const f=await fixture(t),n=await f.add();await f.ready();
 const schema=await f.rows("PRAGMA table_info('v2_outbound_orders')");assert.ok(!schema.some(x=>x.name==='is_deleted'));
 const body=await f.body([n]),before=await f.snapshot(),result=await f.print(body);assert.equal(result.ok,true,JSON.stringify(result));assert.equal(result.print_requested,true);
 assert.equal(await f.snapshot(),before);assert.deepEqual(await f.rows("PRAGMA table_info('v2_outbound_orders')"),schema);
 const events=await f.events();assert.equal(events.length,1);assert.equal(events[0].record_id,n.id);assert.deepEqual(JSON.parse(events[0].result_json),result);assert.equal(JSON.parse(events[0].after_json).document_key,body.items[0].document_key);assert.equal(JSON.parse(events[0].after_json).requirement_version,1);
});
test('existing non-cancelled outbound lifecycle states retain their print permission',async t=>{
 const f=await fixture(t),needs=[];for(const state of ['pending_issue','issued','ready_to_ship','shipped','completed'])needs.push(await f.add('outbound',state));await f.ready();
 const before=await f.snapshot();for(const n of needs){const result=await f.print(await f.body([n]));assert.equal(result.ok,true,JSON.stringify(result));}
 assert.equal((await f.events()).length,needs.length);assert.equal(await f.snapshot(),before);
});
test('cancelled and physically deleted outbound sources still reject new print requests',async t=>{
 const f=await fixture(t),cancelled=await f.add('outbound','cancelled'),removed=await f.add('outbound','cancelled');await f.ready();
 const deletion=await f.ok('office','v2_outbound_order_delete',{id:removed.source_id});assert.equal(deletion.deleted.order,1);
 const before=await f.snapshot();for(const n of [cancelled,removed]){const result=await f.print(await f.body([n]));assert.equal(result.ok,false);assert.match(result.error,/取消|删除|缺失/);assert.doesNotMatch(result.error,/no such column/);}
 assert.equal((await f.events()).length,0);assert.equal(await f.snapshot(),before);
});
test('inbound soft deletion and cancellation stay protected alongside valid mixed-source printing',async t=>{
 const f=await fixture(t),active=await f.add('inbound'),deleted=await f.add('inbound'),cancelled=await f.add('inbound','cancelled'),outbound=await f.add();
 await f.run('UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?',deleted.source_id);await f.ready();
 const before=await f.snapshot();assert.equal((await f.print(await f.body([active,outbound]))).ok,true);
 for(const n of [deleted,cancelled])assert.equal((await f.print(await f.body([n]))).ok,false);
 assert.equal((await f.events()).length,2);assert.equal(await f.snapshot(),before);
});
test('receipt replay remains historical after cancellation and conflicting content cannot add an event',async t=>{
 const f=await fixture(t),n=await f.add(),other=await f.add();await f.ready();const body=await f.body([n]),saved=await f.print(body);assert.equal(saved.ok,true);
 await f.run("UPDATE v2_outbound_orders SET status='cancelled' WHERE id=?",n.source_id);
 assert.deepEqual(await f.print(body),saved);assert.equal((await f.print(await f.body([n]))).ok,false);
 assert.equal((await f.print({...await f.body([other]),client_req_id:body.client_req_id})).ok,false);assert.equal((await f.events()).length,1);
});
test('changed need revision or document label still refuses stale outbound print requests',async t=>{
 const f=await fixture(t),a=await f.add(),b=await f.add();await f.ready();const oldA=await f.body([a]),oldB=await f.body([b]);
 await f.run('UPDATE sop_records SET revision=revision+1 WHERE id=?',a.id);await f.run("UPDATE v2_outbound_orders SET display_no='SYN-NEW-SOURCE-LABEL' WHERE id=?",b.source_id);
 for(const body of [oldA,oldB]){const result=await f.print(body);assert.equal(result.ok,false);assert.match(result.error,/已更新|已变更/);}
 assert.equal((await f.events()).length,0);assert.equal((await f.print(await f.body([a,b]))).ok,true);
});
test('failed mixed print batch preserves all original records and retries as one atomic audit batch',async t=>{
 const f=await fixture(t),a=await f.add(),b=await f.add('inbound');await f.ready();const body=await f.body([a,b]),before=await f.snapshot();
 await f.run(`CREATE TRIGGER synthetic_print_failure BEFORE INSERT ON sop_events WHEN NEW.action='sop_need_print_request' AND NEW.record_id='${b.id}' BEGIN SELECT RAISE(ABORT,'synthetic print failure'); END`);
 assert.equal((await f.print(body)).ok,false);assert.equal((await f.events()).length,0);assert.equal(await f.snapshot(),before);
 await f.run('DROP TRIGGER synthetic_print_failure');const saved=await f.print(body);assert.equal(saved.ok,true);assert.deepEqual(await f.print(body),saved);assert.equal((await f.events()).length,2);assert.equal(await f.snapshot(),before);
});
test('a real dispatcher with only direct-ship permission cannot print the bulk historical origin',async t=>{
 const f=await fixture(t),n=await f.add(),p=(await f.ok('office','sop_attendance_register',{name:'合成限直发派审员',agency:'가온'})).person;
 await f.ok('office','sop_attendance_checkin',{badge:p.badgeId,agency:p.agency});await f.ok('office','sop_daily_dispatch_update',{person_id:p.id,badge:p.badgeId,enabled:true,departments:['direct_ship'],version:0,reason:'合成打印权限回归'});
 const login=await f.ok('A','sop_login',{badge:p.badgeId});assert.deepEqual(login.user.departments,['direct_ship']);await f.ready();const body=await f.body([n]),before=await f.snapshot();
 assert.equal((await f.print(body,'A')).ok,false);assert.equal((await f.events()).length,0);assert.equal(await f.snapshot(),before);assert.equal((await f.print(body)).ok,true);
});
