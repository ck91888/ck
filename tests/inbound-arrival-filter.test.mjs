import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const request=async(action,data={})=>worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 const call=async(action,data={})=>{const r=await(await request(action,data)).json();assert.equal(r.ok,true,r.error||r.message);return r;};
 const plan=async(arrival,extra={})=>{const p=await call('v2_inbound_plan_create',{customer:'QA 合成客户',biz_classes:['bulk'],expected_arrival:arrival,lines:[{unit_type:'carton',planned_qty:1}],...extra});return DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(p.id);};
 return {DB,request,call,plan,list:b=>call('v2_inbound_plan_list',b),export:b=>call('v2_inbound_plan_export',b)};
}
const ids=r=>r.items.map(p=>p.id).sort();
test('expected arrival filters the business field independently of creation and actual unloading',async()=>{
 const f=await fixture();try{
 const a=await f.plan('2026-10-13'),b=await f.plan('2026-10-14');
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-14',created_at='2026-10-14T00:00:00Z',unload_completed_at='2026-10-14T00:00:00Z' WHERE id=?").run(a.id);
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-13',created_at='2026-10-13T00:00:00Z',unload_completed_at='2026-10-13T00:00:00Z' WHERE id=?").run(b.id);
 assert.deepEqual(ids(await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'})),[a.id]);
 }finally{f.DB.raw.close();}
});
test('inclusive range, one-sided ranges, undated exclusion and clearing retain the original data',async()=>{
 const f=await fixture();try{
 const plans=[];for(const d of ['2026-10-12','2026-10-13','2026-10-14','2026-10-15',''])plans.push(await f.plan(d));
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET expected_arrival=NULL WHERE id=?').run(plans[4].id);
 const before=f.DB.raw.prepare('SELECT id,expected_arrival FROM v2_inbound_plans ORDER BY id').all();
 assert.deepEqual(ids(await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-14'})),plans.slice(1,3).map(p=>p.id).sort());
 assert.equal((await f.list({expected_arrival_from:'2026-10-14'})).total,2);assert.equal((await f.list({expected_arrival_to:'2026-10-13'})).total,2);
 assert.equal((await f.list({expected_arrival_from:'',expected_arrival_to:''})).total,5);
 assert.deepEqual(f.DB.raw.prepare('SELECT id,expected_arrival FROM v2_inbound_plans ORDER BY id').all(),before);
 }finally{f.DB.raw.close();}
});
test('Korean business dates agree with existing normalization for legacy dates and time-zone boundaries',async()=>{
 const f=await fixture();try{
 const values=['2026-10-12T14:59:59Z','2026-10-12T15:00:00Z','2026-10-13T14:59:59.999Z','2026-10-13T15:00:00Z','2026-10-13T23:59:59.999','2026/10/13','2026-10-12T23:00:00-04:00','2026-10-14T01:00:00+12:00'];
 const plans=[];for(const value of values){const p=await f.plan(value);f.DB.raw.prepare('UPDATE v2_inbound_plans SET expected_arrival=? WHERE id=?').run(value,p.id);plans.push(p);}
 const result=await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'});assert.deepEqual(ids(result),[1,2,4,5,6,7].map(i=>plans[i].id).sort());
 const api=await f.plan('2026-10-12T15:00:00Z');assert.equal(api.expected_arrival,'2026-10-13');
 }finally{f.DB.raw.close();}
});
test('customer, business, status, accounting, creation and unloading compose with page totals and CSV export',async()=>{
 const f=await fixture();try{
 const targets=[];for(let i=0;i<3;i++)targets.push(await f.plan('2026-10-13'));
 await f.plan('2026-10-14');await f.plan('2026-10-13',{customer:'QA 其他客户'});await f.plan('2026-10-13',{biz_classes:['return']});const complete=await f.plan('2026-10-13');f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='completed' WHERE id=?").run(complete.id);
 for(const p of targets)f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-01',accounted=1,unload_completed_at='2026-10-12T15:00:00Z' WHERE id=?").run(p.id);
 const filter={expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13',customer_keyword:'合成',biz_class:'bulk',status:'pending',accounted:'1',start_date:'2026-10-01',end_date:'2026-10-01',unload_done_date_from:'2026-10-13',unload_done_date_to:'2026-10-13',limit:2};
 const first=await f.list(filter),second=await f.list({...filter,offset:2});assert.equal(first.total,3);assert.equal(first.page_count,2);assert.equal(first.items.length,2);assert.equal(second.total,3);assert.equal(second.items.length,1);assert.deepEqual([...ids(first),...ids(second)].sort(),targets.map(p=>p.id).sort());
 const exported=await f.export({...filter,limit:10000});assert.equal(exported.rows.length,3);assert.deepEqual(exported.rows.map(p=>p['入库单号']).sort(),targets.map(p=>p.display_no).sort());assert.ok(exported.rows.every(p=>p['预计到达日期']==='2026-10-13'));
 }finally{f.DB.raw.close();}
});
test('invalid dates or a reversed range fail both list and export instead of silently widening the search',async()=>{
 const f=await fixture();try{await f.plan('2026-10-13');for(const data of [{expected_arrival_from:'2026-02-30'},{expected_arrival_to:'2026-13-01'},{expected_arrival_from:'2026-10-14',expected_arrival_to:'2026-10-13'},{expected_arrival_from:"2026-10-13' OR 1=1"}])for(const action of ['v2_inbound_plan_list','v2_inbound_plan_export']){const r=await f.request(action,data);assert.equal(r.status,400);assert.equal((await r.json()).ok,false);}assert.equal((await f.list({})).total,1);
 }finally{f.DB.raw.close();}
});
test('arrival filtering preserves cancelled, deleted and return-session exclusions',async()=>{
 const f=await fixture();try{
 const good=await f.plan('2026-10-13'),cancelled=await f.plan('2026-10-13'),deleted=await f.plan('2026-10-13'),session=await f.plan('2026-10-13');
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(cancelled.id);f.DB.raw.prepare('UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?').run(deleted.id);f.DB.raw.prepare("UPDATE v2_inbound_plans SET source_type='return_session' WHERE id=?").run(session.id);
 const b={expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'};assert.deepEqual(ids(await f.list(b)),[good.id]);assert.equal((await f.export(b)).rows.length,1);assert.deepEqual(ids(await f.list({...b,status:'cancelled'})),[cancelled.id]);
 }finally{f.DB.raw.close();}
});
