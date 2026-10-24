import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
export async function loadFixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true'};
 const raw=body=>worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}),env);
 const call=async(action,data={})=>(await raw({action,client_req_id:crypto.randomUUID(),...data})).json();
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const get=async id=>(await ok('sop_get',{id})).record;
 const change=async(action,id,data={})=>ok(action,{id,revision:(await get(id)).revision,...data});
 let number=0;
 async function order(customer,qty,unit='箱',{review=true,issue=true}={}){
  const k=++number,n=await ok('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'FIXTURE-STOCK-'+k,title:'虚拟装货作业 '+k,customer,instructions:'按纸质作业单核对',planned_quantity:qty,planned_unit:unit});
  const ob=await ok('v2_outbound_order_create',{customer,biz_class:'bulk',outbound_mode:'customer_pickup',expected_ship_at:'2026-10-03',sop_existing_need_id:n.id,sop_need_revision:n.revision,sop_link_quantity:qty,po_no:'PO-FIXTURE-'+k});
  if(issue)await ok('v2_outbound_order_update_status',{id:ob.id,status:'issued'});
  if(review){const staff=[{id:'PREP-'+k,name:'虚拟前序操作员'}],task=await ok('sop_task_create',{department:'bulk',title:'虚拟前序作业',job_type:'bulk_op',need_id:n.id,workers:staff,lead_id:staff[0].id,estimated_minutes:10});
   await change('sop_task_start',task.id);await change('sop_task_complete_review',task.id,{result:{quantity:qty,unit},decision:'pass',reason:'虚拟验收完成'});
  }
  return {...ob,need_id:n.id,customer,box_count:unit==='箱'?qty:0,pallet_count:unit==='托'?qty:0};
 }
 const staff=[{id:'LOAD-A',name:'虚拟装货甲'},{id:'LOAD-B',name:'虚拟装货乙'}];
 const payload=(orders,workers=staff,requestId=crypto.randomUUID())=>({payload:{action:'v2_outbound_load_start',order_ids:orders.map(o=>o.id),client_req_id:requestId,vehicle_no:'测试车123',driver_name:'虚拟司机'},workers,lead_id:workers[0].id,estimated_minutes:30,labor_department:'bulk'});
 const start=(orders,workers=staff)=>ok('sop_native_start',payload(orders,workers));
 const finishBody=(job,orders)=>({job_id:job.job_id,worker_id:staff[0].id,complete_job:true,order_results:orders.map(o=>({order_id:o.id,box_count:o.box_count,pallet_count:o.pallet_count}))});
 await ok('sop_identity');return {DB,env,raw,call,ok,get,change,order,staff,payload,start,finishBody};
}
