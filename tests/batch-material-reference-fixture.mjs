// Authenticated Worker and synthetic in-memory storage; optional local workerd D1.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
export async function materialReferenceFixture(t){
 t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});
 const original=database(),objects=new Map(),user={id:'SYN-MATERIAL-MANAGER',name:'合成资料管理员',role:'manager',scope:'office'};
 const native=process.env.CK_P1_NATIVE_D1==='true'?await localD1(original.raw):null,DB=native||original;
 t.after(async()=>{await native?.close();original.raw.close();});
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ACCEPT_NEW:'true',SOP_USERS_JSON:JSON.stringify([{...user,key:'synthetic-material-reference'}]),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body,httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(){throw Error('Synthetic guard: reference changes must not delete objects');}}};
 let cookie='';
 const raw=async body=>{const multi=body instanceof FormData,response=await worker.fetch(new Request('https://synthetic.invalid/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie),...(!multi?{'Content-Type':'application/json'}:{})},body:multi?body:JSON.stringify(body)}),env);if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];return rememberOperation(await response.json(),cookie);};
 const call=(action,body={})=>raw({action,client_req_id:crypto.randomUUID(),...body}),ok=async(action,body={})=>{const result=await call(action,body);assert.equal(result.ok,true,action+': '+result.error);return result;};
 const query=(sql,...args)=>env.DB.prepare(sql).bind(...args),first=(sql,...args)=>query(sql,...args).first();
 await ok('sop_login',{sop_key:'synthetic-material-reference'});await confirmOperation(ok,user.name);
 const plan=await ok('v2_inbound_plan_create',{customer:'合成资料客户',biz_classes:['bulk']});
 const need=async(extra={})=>ok('sop_need_create',{customer:'合成资料客户',title:'合成资料作业',instructions:'仅测试资料引用并发',source_type:'inbound',source_id:plan.id,planned_quantity:10,planned_unit:'箱',department:'bulk',reason:'合成追加作业',...extra});
 const a=await need(),form=new FormData();
 for(const[k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:plan.id,need_id:a.id,attachment_category:'batch_work_material',client_req_id:crypto.randomUUID()}))form.set(k,v);
 form.set('file',new File(['synthetic material row'],'合成资料.csv',{type:'text/csv'}));const file=await raw(form);assert.equal(file.ok,true,file.error);
 const current=async id=>{const row=await first('SELECT * FROM sop_records WHERE id=?',id);return {...row,data:JSON.parse(row.state)};};
 const group=async(id,extra={})=>({action:'sop_cargo_groups_save',id,revision:(await current(id)).revision,groups:[{name:'合成组',range:'SYN001',box_count:1,attachment_ids:[file.id]}],client_req_id:crypto.randomUUID(),...extra});
 const remove=(extra={})=>({action:'sop_batch_work_material_remove',id:a.id,attachment_id:file.id,material_revision:0,client_req_id:crypto.randomUUID(),...extra});
 const snapshot=async()=>{const tables=['sop_records','sop_events','v2_inbound_plans','v2_inbound_plan_lines','v2_attachments','ck_batch_material_state','ck_batch_material_events','ck_inbound_document_versions','v2_outbound_orders','v2_outbound_order_change_logs','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results'];const result={};for(const table of tables)result[table]=(await query('SELECT * FROM '+table+' ORDER BY rowid').all()).results;return JSON.stringify(result);};
 return {DB,env,user,objects,raw,call,ok,query,first,plan,a,file,need,current,group,remove,snapshot,get cookie(){return cookie;}};
}
export function holdMaterialBatch(f,predicate){
 const db=f.env.DB;let reached,release,held=false;
 const ready=new Promise(resolve=>reached=resolve),gate=new Promise(resolve=>release=resolve);
 f.env.DB={prepare:db.prepare.bind(db),async batch(statements){if(!held&&predicate(statements)){held=true;reached();await gate;}return db.batch(statements);}};
 return {ready,release,restore(){f.env.DB=db;}};
}
