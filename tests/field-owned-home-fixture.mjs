// Real Worker and badge sessions; all records live in SQLite :memory: only.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
export async function ownedHomeFixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-owned-home-fixture')},cookies={office:'',A:'',B:''};
 async function call(scope,action,body={}){const response=await worker.fetch(new Request('http://127.0.0.1'+(scope==='office'?'/api':'/001/api'),{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookies[scope],...operationHeaders(cookies[scope])},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);const next=response.headers.getSetCookie().find(x=>x.startsWith(scope==='office'?'ck_office_access=':'ck_field_access='));if(next)cookies[scope]=next.split(';')[0];return rememberOperation(await response.json(),cookies[scope]);}
 const ok=async(scope,action,body={})=>{const result=await call(scope,action,body);assert.equal(result.ok,true,action+': '+result.error);return result;};
 await ok('office','sop_login',{sop_key:'synthetic-owned-home-fixture'});await confirmOperation((a,b)=>ok('office',a,b),'合成任务归属验收员');
 const people={};for(const key of ['A','B']){const p=(await ok('office','sop_attendance_employee_register',{employeeNo:'OWNED-'+key,name:'合成派审'+key,department:'bulk'})).person;p.record=(await ok('office','sop_attendance_checkin',{badge:p.badgeId})).record;people[key]=p;await ok(key,'sop_login',{badge:p.badgeId});}
 let workerSequence=0;const person=()=>ok('office','sop_attendance_checkin',{name:'合成操作员'+(++workerSequence),agency:'가온'}).then(x=>x.record);
 const need=title=>ok('office','sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'SYNTHETIC-'+crypto.randomUUID(),customer:'合成任务归属客户',title,instructions:'隔离任务归属和扫码交接验证'});
 const task=async(scope,title,crew)=>{const n=await need(title),w=crew||await person(),created=await ok(scope,'sop_task_dispatch',{need_id:n.id,department:'bulk',title,job_type:'bulk_op',workers:[{id:w.badgeId,name:w.name}],lead_id:w.badgeId,estimated_minutes:15});return {...created,needId:n.id};};
 const record=id=>{const r=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(id);return {id:r.id,revision:r.revision,...JSON.parse(r.state)};};
 const clocks=()=>DB.raw.prepare('SELECT * FROM v2_ops_job_workers ORDER BY rowid').all();
 const mine=scope=>ok(scope,'sop_dispatch_list',{view:'mine'});
 return {DB,env,cookies,people,call,ok,person,need,task,record,clocks,mine,worker};
}
