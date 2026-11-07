// Synthetic local-only actual Worker fixture; no warehouse credentials or data.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
export async function mixedShippingFixture(t){
 const DB=database();t?.after(()=>DB.raw.close());
 const users=[['manager','manager'],['service','service'],['reviewer','reviewer'],['dispatcher','dispatcher'],['viewer','viewer'],['other','reviewer']].map(([id,role])=>({id:'QA-'+id,name:'合成 '+id,role,scope:'office',departments:id==='other'?['import']:['bulk'],key:'synthetic-shipping-'+id}));
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_USERS_JSON:JSON.stringify(users.map(u=>({...u,role:'manager'})))},cookies={};
 const call=async(role,action,body={})=>{const r=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{Cookie:cookies[role]||'',...operationHeaders(cookies[role]),'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);if(r.headers.has('set-cookie'))cookies[role]=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookies[role]);};
 const ok=async(role,action,body={})=>{const r=await call(role,action,body);assert.equal(r.ok,true,JSON.stringify(r));return r;};
 for(const user of users){const role=user.id.slice(3);await ok(role,'sop_login',{sop_key:user.key});// Seed synthetic role claims in the local session; configured staging office login is manager-only.
 DB.raw.prepare("UPDATE ck_access_sessions SET role=? WHERE user_id=? AND scope='office'").run(user.role,user.id);await confirmOperation((a,b)=>call(role,a,b),user.name);}
 const get=async id=>(await ok('manager','sop_get',{id})).record;
 const change=async(role,action,id,body={})=>call(role,action,{id,revision:(await get(id)).revision,...body});
 async function need(extra={}){
  const n=await ok('manager','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTH-ONLY',customer:'合成出货客户',title:'合成混合包装作业',instructions:'按实际核实独立出货包装',planned_quantity:40,planned_unit:'箱',...extra});
  const id='QA-W-'+crypto.randomUUID(),job=await ok('dispatcher','sop_task_create',{department:'bulk',title:'合成打托作业',job_type:'bulk_op',need_id:n.id,workers:[{id,name:'合成操作员'}],lead_id:id,estimated_minutes:5});
  assert.equal((await change('dispatcher','sop_task_start',job.id)).ok,true);
  const done=await change('dispatcher','sop_task_complete_review',job.id,{decision:'pass',reason:'合成实绩审核',result:{quantity:5,unit:'托',pallet_count:5,packed_count:1,used_carton_small_count:1}});assert.equal(done.ok,true,done.error);
  return get(n.id);
 }
 const booking=(quantity,unit,extra={})=>({quantity,unit,expected_ship_at:'2026-10-20',outbound_mode:unit==='箱'?'milk_express':'milk_pallet',...extra});
 const book=async(id,rows,extra={})=>change('manager','v2_outbound_order_batch_create',id,{outbounds:rows,...extra});
 const propose=async(id,components=[{quantity:5,unit:'托'},{quantity:1,unit:'箱'}],extra={})=>change('service','sop_shipping_output_propose',id,{components,reason:'合成现场核实：另有独立散箱，非托内箱或耗材',...extra});
 const review=async(id,extra={})=>change('reviewer','sop_shipping_output_review',id,{change_id:(await get(id)).shipping_output_change.id,decision:'pass',reason:'合成仓库复核独立出货包装',...extra});
 const availability=async id=>(await ok('service','sop_work_need_search',{search:id})).items.find(n=>n.id===id);
 return {DB,env,worker,users,cookies,call,ok,get,change,need,booking,book,propose,review,availability};
}
