import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const tally='inbound_direct_tally';
export async function partialArrivalFixture(t,{authenticated=false}={}){
 const seed=database();let DB=seed;if(process.env.CK_PARTIAL_NATIVE_D1==='true'){DB=await localD1(seed.raw);seed.raw.close();t.after(()=>DB.close());}else t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 let cookie='';
 if(authenticated)Object.assign(env,{SOP_ACCESS_CONTROL:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID(),SOP_PUBLIC_TEST_ACCESS:'false'});
 const call=async(action,data={},target=env)=>{const r=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),target);const next=r.headers.getSetCookie().find(x=>x.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];return rememberOperation(await r.json(),cookie);};
 const ok=async(action,data)=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+JSON.stringify(r));return r;};
 if(authenticated){await ok('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(ok,'合成分批开关核验员');}
 const codes=['999001770001','999001770002'],code='SYNTH-PARTIAL-EXT';
 const create=extra=>ok('v2_inbound_plan_create',{customer:'合成部分到仓客户',biz_classes:['direct_ship'],external_inbound_no:code,lines:[{unit_type:'courier',tracking_nos:codes}],...extra});
 let batch,receiver;
 async function receive(index){
  env.SOP_ATTENDANCE_ENABLED='true';
  try{
   if(!receiver){receiver=(await ok('sop_attendance_checkin',{name:'合成快递扫描员',agency:'가온'})).record;batch=(await ok('sop_courier_receive',{operation:'start',workers:[{id:receiver.badgeId,name:receiver.name}],lead_id:receiver.badgeId})).batch;}
   return await ok('sop_courier_receive',{tracking_no:codes[index],owner:'8-1',batch_id:batch.id,scanner_badge:receiver.badgeId});
  }finally{env.SOP_ATTENDANCE_ENABLED='false';}
 }
 let serial=0;
 const start=(p,type=tally,external=code,request)=>{const id='SYNTH-TALLY-'+(++serial);return call('sop_native_start',{payload:{action:'v2_inbound_job_start',plan_id:p.id,external_inbound_no:external,job_type:type,biz_class:type==='inbound_bulk'?'bulk':'direct_ship',client_req_id:request||crypto.randomUUID()},workers:[{id,name:'合成理货员 '+serial}],lead_id:id,estimated_minutes:15});};
 const finish=(j,quantity,extra={})=>call('v2_inbound_job_finish',{job_id:j.job_id,worker_id:j.lead.id,complete_job:true,result_lines:quantity===undefined?[]:[{unit_type:'courier',putaway_qty:quantity}],...extra});
 const detail=p=>ok('v2_inbound_plan_detail',{id:p.id});
 return {DB,env,call,ok,codes,code,create,receive,start,finish,detail};
}
