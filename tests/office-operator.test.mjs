import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
async function fixture(t){
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('isolated-name-label-only')};let cookie='';
 const call=async(action,data={},context='')=>{const response=await worker.fetch(new Request('https://label.fixture/api',{method:'POST',headers:{Origin:'https://label.fixture','Content-Type':'application/json',Cookie:cookie,'X-CK-Operation-Context':context},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(action==='sop_login'&&response.ok)cookie=response.headers.get('set-cookie').split(';')[0];return {status:response.status,...await response.json()};};
 await call('sop_login',{sop_key:'isolated-name-label-only'});
 const identity=()=>call('sop_identity');
 const confirm=async(name)=>{const current=await identity();return call('sop_operator_confirm',{name},current.user.operation_context.id);};
 return {DB,env,call,identity,confirm};
}
test('legacy office sessions must confirm a nonempty label before plan writes; invalid names leave no audit',async t=>{
 const f=await fixture(t),identity=await f.identity(),ctx=identity.user.operation_context;
 assert.equal(ctx.name,'');assert.equal(identity.user.id,'ck-office-admin');assert.equal(identity.user.account_name,'管理员');assert.equal(identity.user.role,'manager');
 for(const name of ['', '   ', '一'.repeat(41),'甲\u0000乙','甲\u202e乙'])assert.equal((await f.call('sop_operator_confirm',{name},ctx.id)).ok,false);
 for(const id of ['',ctx.id]){const r=await f.call(' v2_inbound_plan_create ',{customer:'isolated',biz_classes:['bulk'],created_by:'forged'},id);assert.equal(r.status,409);assert.equal(r.operator_context_changed,true);}
 assert.equal(f.DB.raw.prepare('SELECT COUNT(*) n FROM ck_plan_operation_audit').get().n,0);
 const a=await f.confirm('  隔离甲 <한>&"  ');assert.equal(a.user.name,'隔离甲 <한>&"');assert.equal(a.user.operation_context.self_entered,true);assert.equal(a.user.id,identity.user.id);assert.deepEqual(a.user.departments,identity.user.departments);
 assert.ok(!JSON.stringify(a.user).includes('session_hash'));
});
test('server labels attribute actual inbound edits and preserve original creation and historical data',async t=>{
 const f=await fixture(t),a=(await f.confirm('隔离甲')).user.operation_context.id;
 const plan=await f.call('v2_inbound_plan_create',{customer:'isolated',biz_classes:['bulk'],created_by:'管理员',lines:[]},a);assert.equal(plan.ok,true,plan.error);
 const b=(await f.confirm('隔离乙')).user.operation_context.id;
 const old=await f.call('v2_inbound_plan_update',{id:plan.id,biz_classes:['bulk'],remark:'must not save'},a);assert.equal(old.status,409);
 const edited=await f.call('v2_inbound_plan_update',{id:plan.id,biz_classes:['bulk'],remark:'saved by B',by:'forged',created_by:'forged'},b);assert.equal(edited.ok,true,edited.error);
 const detail=await f.call('v2_inbound_plan_detail',{id:plan.id});assert.equal(detail.plan.created_by,'隔离甲');assert.equal(detail.plan.remark,'saved by B');assert.deepEqual(detail.operation_audit.map(x=>[x.kind,x.actor_name,x.actor_id]),[['update','隔离乙','ck-office-admin'],['create','隔离甲','ck-office-admin']]);
 // A real historical row remains unchanged and has no fabricated events.
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET id=?,created_by=? WHERE id=?').run('HISTORY-NAME','legacy label',plan.id);
 const historical=await f.call('v2_inbound_plan_detail',{id:'HISTORY-NAME'});assert.equal(historical.plan.created_by,'legacy label');assert.deepEqual(historical.operation_audit,[]);
});
test('outbound creation, edits and old change logs use the confirmed server label; retries do not duplicate audit',async t=>{
 const f=await fixture(t),ctx=(await f.confirm('가상작업자')).user.operation_context.id,req=crypto.randomUUID(),body={client_req_id:req,customer:'isolated',biz_class:'bulk',outbound_mode:'customer_pickup',created_by:'forged'};
 const created=await f.call('v2_outbound_order_create',body,ctx);assert.equal(created.ok,true,created.error);assert.equal((await f.call('v2_outbound_order_create',body,ctx)).id,created.id);
 const edited=await f.call('v2_outbound_order_update',{id:created.id,remark:'actual edit',by:'管理员',actor:'forged'},ctx);assert.equal(edited.ok,true,edited.error);
 const d=await f.call('v2_outbound_order_detail',{id:created.id});assert.equal(d.order.created_by,'가상작업자');assert.equal(d.order.last_modified_by,'가상작업자');assert.equal(d.change_logs[0].changed_by,'가상작업자');assert.equal(d.operation_audit.length,2);
});
test('concurrent confirmations have one winner; relogin rejects old page writes even when its name is identical',async t=>{
 const f=await fixture(t),start=(await f.identity()).user.operation_context.id;
 const r=await Promise.all([f.call('sop_operator_confirm',{name:'隔离甲'},start),f.call('sop_operator_confirm',{name:'隔离乙'},start)]);assert.equal(r.filter(x=>x.ok).length,1);assert.equal(r.filter(x=>x.status===409).length,1);
 const old=(await f.identity()).user.operation_context.id;
 await f.call('sop_logout');await f.call('sop_login',{sop_key:'isolated-name-label-only'});const fresh=(await f.confirm('隔离甲')).user.operation_context.id;assert.notEqual(old,fresh);
 assert.equal((await f.call('v2_inbound_plan_create',{customer:'isolated',biz_classes:['bulk']},old)).status,409);
 assert.equal((await f.call('v2_inbound_plan_create',{customer:'isolated',biz_classes:['bulk']},fresh)).ok,true);
});
test('audit failure rolls back an outbound edit and its existing change log',async t=>{
 const f=await fixture(t),ctx=(await f.confirm('隔离事务测试员')).user.operation_context.id;
 const p=await f.call('v2_outbound_order_create',{customer:'isolated',biz_class:'bulk',outbound_mode:'customer_pickup'},ctx);
 f.DB.raw.exec("CREATE TRIGGER isolated_audit_failure BEFORE INSERT ON ck_plan_operation_audit WHEN NEW.kind='update' BEGIN SELECT RAISE(ABORT,'isolated audit failure'); END");
 const changed=await f.call('v2_outbound_order_update',{id:p.id,remark:'must roll back'},ctx);assert.equal(changed.ok,false);
 const detail=await f.call('v2_outbound_order_detail',{id:p.id});assert.equal(detail.order.remark,'');assert.equal(detail.change_logs.length,0);assert.equal(detail.operation_audit.length,1);
});

test('linked work events preserve the account ID and use the confirmed operation label',async t=>{
 const f=await fixture(t),ctx=(await f.confirm('隔离作业计划员')).user.operation_context.id;
 const n=await f.call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'ISOLATED-NAME-STOCK',title:'隔离姓名作业',customer:'isolated',instructions:'isolated audit',planned_quantity:10,planned_unit:'箱',created_by:'forged',operator_name:'管理员'},ctx);assert.equal(n.ok,true,n.error);
 const event=f.DB.raw.prepare('SELECT actor_id,actor_name FROM sop_events WHERE record_id=?').get(n.id);assert.deepEqual({...event},{actor_id:'ck-office-admin',actor_name:'隔离作业计划员'});
});
