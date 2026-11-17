// Synthetic rows in memory/local workerd D1 only; no remote service.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
export const DAY='2026-11-02',NEXT='2026-11-03',AS_OF='2026-11-03T05:00:00+09:00';
export async function cutoffDashboardFixture(t,{native=process.env.CK_P1_NATIVE_D1==='true'}={}){
 t.mock.timers.enable({apis:['Date'],now:Date.parse(AS_OF)});
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};let d1;t.after(async()=>{await d1?.close();DB.raw.close();});
 const request=async(action,data={},changes={},headers={})=>{const response=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',...headers},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),{...env,...changes});return{response,data:await response.json()};};
 const api=async(action,data={},changes={},headers={})=>{const result=await request(action,data,changes,headers);assert.equal(result.data.ok,true,JSON.stringify(result.data));return result.data;};await api('sop_identity');
 const job=({id='SYN-CUTOFF',kind='dispatch',department='bulk',type='bulk_op',start=DAY+'T09:00:00+09:00',status='working'}={})=>{DB.raw.prepare('INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_at,updated_at,active_worker_count) VALUES(?,?,?,?,?,?,1)').run(id,type,department,status,new Date(start).toISOString(),new Date(start).toISOString());DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,1,?,?,?)').run(id,kind,department,JSON.stringify({title:'Synthetic cutoff '+id,owner_id:'SYN-MANAGER',owner:'Synthetic manager',status,job_type:type,created_at:new Date(start).toISOString(),workers:[{id:'SYN-WORKER',name:'Synthetic worker'}],lead_id:'SYN-WORKER'}),new Date(start).toISOString());return id;};
 const clock=({id='SYN-CLOCK',jobId='SYN-CUTOFF',badge='SYN-WORKER',start=DAY+'T09:00:00+09:00',end='',reason='',minutes=17.5}={})=>DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked,leave_reason) VALUES(?,?,?,?,?,?,?,?)').run(id,jobId,badge,'Synthetic '+badge,new Date(start).toISOString(),end?new Date(end).toISOString():'',minutes,reason);
 const bind=async()=>{if(native){d1=await localD1(DB.raw);env.DB=d1;}};
 const rows=async(sql,...args)=>(await env.DB.prepare(sql).bind(...args).all()).results;
 const snapshot=async()=>{const result={};for(const{name}of await rows("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name"))result[name]=await rows('SELECT * FROM "'+name+'" ORDER BY rowid');return JSON.stringify(result);};
 return {DB,env,request,api,job,clock,bind,rows,snapshot};
}
