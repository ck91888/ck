import test from 'node:test';import assert from 'node:assert/strict';import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
test('actual Worker keeps legacy flow with flag off and creates canonical outbound atomically when on',async()=>{
 const DB=database(),env={DB,ADMINKEY:'test-admin',OPSKEY:'test-ops',SOP_UPGRADE_ENABLED:'false'};
 const request=async b=>{const r=await worker.fetch(new Request('http://test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({k:'test-admin',...b})}),env);return r.json();};
 const payload={action:'v2_outbound_order_create',customer:'C',biz_class:'bulk',outbound_mode:'customer_pickup',uses_stock_operation:1,instruction:'贴标',created_by:'测试客服',expected_ship_at:'2026-09-20',lines:[{sku:'SKU-A',quantity:10}]};
 const a=await request({...payload,client_req_id:'old'});assert.equal(a.ok,true,a.error);assert.equal(DB.raw.prepare('SELECT count(*) n FROM sop_records').get().n,0);
 env.SOP_UPGRADE_ENABLED='true';env.SOP_AUTO_OUTBOUND='true';env.SOP_ROLLOUT_DEPARTMENTS='bulk';
 const b=await request({...payload,client_req_id:'new'});assert.equal(b.ok,true,b.error);assert.equal(DB.raw.prepare('SELECT count(*) n FROM sop_records').get().n,1);
 const retry=await request({...payload,client_req_id:'new'});assert.equal(retry.id,b.id);assert.equal(DB.raw.prepare('SELECT count(*) n FROM v2_outbound_orders').get().n,2);
 const detail=await request({action:'v2_outbound_order_detail',id:b.id});assert.equal(detail.ok,true,detail.error);assert.equal(detail.sop_needs.length,1);
 const oldJob=await request({action:'v2_ops_job_start',worker_id:'LEGACY-W',worker_name:'旧任务操作员',job_type:'other',flow_stage:'internal',biz_class:'bulk'});assert.equal(oldJob.ok,true,oldJob.error);
 const leave=await request({action:'v2_ops_job_leave',job_id:oldJob.job_id,worker_id:'LEGACY-W'});assert.equal(leave.ok,true,leave.error);
});
