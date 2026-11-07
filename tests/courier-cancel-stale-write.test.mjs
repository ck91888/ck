import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

const C1='301000007101',C2='301000007102';
async function fixture(t,biz='bulk',codes=[C1]){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const call=async(action,data={})=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env)).json();
 const ok=async(action,data={})=>{const result=await call(action,data);assert.equal(result.ok,true,action+': '+(result.error||result.message));return result;};
 const body=(codes=[C1])=>({customer:'QA cancelled stale write',remark:'QA original instruction',biz_classes:[biz],...(biz==='direct_ship'?{external_inbound_no:'QA-STALE-WMS'}:{}),lines:[{unit_type:'courier',tracking_nos:codes,planned_qty:codes.length}]});
 const plan=await ok('v2_inbound_plan_create',body(codes));
 const person=(await ok('sop_attendance_checkin',{name:'QA stale write scanner',agency:'가온'})).record;
 const batch=await ok('sop_courier_receive',{operation:'start',workers:[{id:person.badgeId,name:person.name}],lead_id:person.badgeId});
 const scan=()=>ok('sop_courier_receive',{owner:'8-1',tracking_no:C1,batch_id:batch.batch.id,scanner_badge:person.badgeId});
 const cancel=()=>ok('v2_inbound_plan_cancel',{inbound_plan_id:plan.id,reason:'QA concurrent cancellation'});
 const state=()=>Object.fromEntries(['v2_inbound_plans','v2_inbound_plan_lines','v2_inbound_plan_biz_tasks','ck_courier_plan_items','ck_courier_plan_item_history','ck_courier_receipts','ck_courier_events','ck_inbound_document_versions'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
 return {DB,call,ok,body,plan,scan,cancel,state};
}

// Finish a competing API call after the candidate's preflight but before its
// transaction/write begins. This uses only synthetic, in-memory data.
function beforeWrite(DB,matches,interleave){
 const prepare=DB.prepare.bind(DB),batch=DB.batch.bind(DB);let injected=false;
 const inject=async()=>{if(injected)return;injected=true;DB.prepare=prepare;DB.batch=batch;await interleave();};
 DB.prepare=sql=>{const statement=prepare(sql);for(const method of ['run','all','first']){const execute=statement[method].bind(statement);statement[method]=async(...args)=>{if(matches(statement))await inject();return execute(...args);};}return statement;};
 DB.batch=async statements=>{if(statements.some(matches))await inject();return batch(statements);};
 return ()=>assert.equal(injected,true,'the competing request must commit at the targeted write boundary');
}

test('receipt completion cannot overwrite a cancellation committed after recalculation preflight',async t=>{
 const f=await fixture(t);let afterCancel;
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plans\s+SET\s+status=\?,\s*updated_at=\?/i.test(s.sql),async()=>{await f.cancel();afterCancel=f.state();});
 await f.scan();injected();
 assert.equal(f.DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(f.plan.id).status,'cancelled');
 assert.deepEqual(f.state(),afterCancel,'a stale completion must preserve the committed cancellation and its history');
});

test('automatic receipt disposition cannot change tasks after cancellation commits',async t=>{
 const f=await fixture(t);let afterCancel;
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plan_biz_tasks\s+SET\s+status='completed'/i.test(s.sql),async()=>{await f.cancel();afterCancel=f.state();});
 await f.scan();injected();
 assert.deepEqual(f.state(),afterCancel,'cancelled plan tasks and status must remain unchanged');
});

for(const concurrent of ['cancel','receive'])test('pending edit rechecks '+concurrent+' before atomically replacing header, tasks, lines and waybills',async t=>{
 const f=await fixture(t);let afterWinner;
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plans\s+SET\s+plan_date=/i.test(s.sql),async()=>{await(concurrent==='cancel'?f.cancel():f.scan());afterWinner=f.state();});
 const edited=await f.call('v2_inbound_plan_update',{id:f.plan.id,...f.body([C2]),customer:'QA stale replacement',remark:'QA must roll back',biz_classes:['change_order']});
 injected();assert.equal(edited.ok,false,'stale edits must fail after the competing lifecycle change');
 assert.deepEqual(f.state(),afterWinner,'failure must preserve the winning header, tasks, lines, receipt and original waybill claim');
});

test('header-only edit cannot change an archived cancellation whose active claims were released',async t=>{
 const f=await fixture(t);let afterRebind;
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plans\s+SET\s+plan_date=/i.test(s.sql),async()=>{
  await f.cancel();await f.ok('v2_inbound_plan_create',f.body());afterRebind=f.state();
 });
 const edited=await f.call('v2_inbound_plan_update',{id:f.plan.id,customer:'QA stale header',remark:'QA must roll back',biz_classes:['bulk']});
 injected();assert.equal(edited.ok,false,'the archived plan must be guarded even when it has no active claim rows');
 assert.deepEqual(f.state(),afterRebind);
});

test('receipt correction cannot target stale plans after cancellation and rebind commit',async t=>{
 const f=await fixture(t,'direct_ship'),received=await f.scan();let replacement,afterRebind;
 const injected=beforeWrite(f.DB,s=>/INSERT\s+INTO\s+ck_courier_events/i.test(s.sql),async()=>{
  await f.cancel();replacement=await f.ok('v2_inbound_plan_create',{...f.body(),external_inbound_no:'QA-REPLACEMENT-WMS'});afterRebind=f.state();
 });
 const corrected=await f.call('sop_courier_update',{id:received.item.id,version:received.item.version,tracking_no:C2,owner:'8-2'});
 injected();assert.equal(corrected.ok,false,'a correction whose source claim changed must require a refreshed request');
 assert.deepEqual(f.state(),afterRebind,'both cancelled history and replacement arrival must retain their correct quantities');
 assert.equal(f.DB.raw.prepare('SELECT actual_qty FROM v2_inbound_plan_lines WHERE plan_id=?').get(f.plan.id).actual_qty,1);
 assert.equal(f.DB.raw.prepare('SELECT actual_qty FROM v2_inbound_plan_lines WHERE plan_id=?').get(replacement.id).actual_qty,1);
});

for(const rebind of [false,true])test('receipt correction rechecks destination '+(rebind?'claim identity':'cancellation')+' at commit',async t=>{
 const f=await fixture(t,'direct_ship',[C2]),received=await f.scan();let afterWinner;
 const injected=beforeWrite(f.DB,s=>s.sql.includes("SELECT ?,id,'update'"),async()=>{
  await f.cancel();
  if(rebind)await f.ok('v2_inbound_plan_create',{...f.body([C2]),external_inbound_no:'QA-DESTINATION-REPLACEMENT-WMS'});
  afterWinner=f.state();
 });
 const corrected=await f.call('sop_courier_update',{id:received.item.id,version:received.item.version,tracking_no:C2,owner:'8-2'});
 injected();assert.equal(corrected.ok,false,'a changed or cancelled destination requires a refreshed request');
 assert.deepEqual(f.state(),afterWinner,'failed correction must preserve the receipt, destination and its history');
});

test('force completion cannot leave partial task or quantity writes after cancellation and rebind',async t=>{
 const f=await fixture(t,'direct_ship');await f.scan();let afterRebind;
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plan_biz_tasks/i.test(s.sql)&&s.sql.includes("completion_source='manual_force'"),async()=>{
  await f.cancel();await f.ok('v2_inbound_plan_create',{...f.body(),external_inbound_no:'QA-FORCE-REPLACEMENT-WMS'});afterRebind=f.state();
 });
 const completed=await f.call('v2_inbound_plan_force_complete',{id:f.plan.id,reason:'QA stale manual completion',actual_box_count:7});
 injected();assert.equal(completed.ok,false);
 assert.deepEqual(f.state(),afterRebind,'the failed completion must not change cancelled tasks, quantities or history');
});

test('force completion commits its tasks and both quantities without leaving a transaction marker',async t=>{
 const f=await fixture(t,'direct_ship');await f.scan();
 await f.ok('v2_inbound_plan_force_complete',{id:f.plan.id,reason:'QA complete quantities',actual_box_count:7,actual_pallet_count:2});
 const plan=f.DB.raw.prepare('SELECT status,force_completed FROM v2_inbound_plans WHERE id=?').get(f.plan.id);
 assert.equal(plan.status,'completed');assert.equal(plan.force_completed,1);
 const quantities=f.DB.raw.prepare('SELECT unit_type,actual_qty FROM v2_inbound_plan_lines WHERE plan_id=? ORDER BY unit_type').all(f.plan.id);
 assert.deepEqual(quantities.map(x=>[x.unit_type,x.actual_qty]),[['carton',7],['courier',1],['pallet',2]]);
 const tasks=f.DB.raw.prepare('SELECT status,completion_source FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').all(f.plan.id);
 assert.ok(tasks.length);assert.ok(tasks.every(x=>x.status==='completed'&&x.completion_source==='manual_force'));
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_idempotency_keys WHERE action='inbound_force_commit'").get().n,0);
});

test('two force completions in the same millisecond cannot share the successful transaction marker',async t=>{
 const f=await fixture(t,'direct_ship');await f.scan();let afterWinner;
 const RealDate=globalThis.Date,fixed=RealDate.now();
 globalThis.Date=class extends RealDate {constructor(...args){super(...(args.length?args:[fixed]));}static now(){return fixed;}};
 t.after(()=>{globalThis.Date=RealDate;});
 const injected=beforeWrite(f.DB,s=>/UPDATE\s+v2_inbound_plan_biz_tasks/i.test(s.sql)&&s.sql.includes("completion_source='manual_force'"),async()=>{
  await f.ok('v2_inbound_plan_force_complete',{id:f.plan.id,reason:'QA winning completion',actual_box_count:3,actual_pallet_count:4});afterWinner=f.state();
 });
 const loser=await f.call('v2_inbound_plan_force_complete',{id:f.plan.id,reason:'QA losing completion',actual_box_count:99,actual_pallet_count:88});
 injected();assert.equal(loser.ok,false);
 assert.deepEqual(f.state(),afterWinner,'same timestamp must not authorize the losing request to overwrite the winning quantities');
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_idempotency_keys WHERE action='inbound_force_commit'").get().n,0);
});
