// Real authenticated Worker; synthetic in-memory or local workerd D1 only.
import assert from 'node:assert/strict';import {pathToFileURL} from 'node:url';
import {database} from './d1-adapter.mjs';import {localD1} from './d1-local-runtime.mjs';
import {digest} from '../worker-v2/access-control.js';import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const root=process.env.CK_REVIEW_REPO?pathToFileURL(process.env.CK_REVIEW_REPO.replace(/\/$/,'')+'/'):new URL('../',import.meta.url),{default:worker}=await import(new URL('worker-v2/index.js',root));
export async function agencyFixture(t){
 const original=database(),native=process.env.CK_AGENCY_NATIVE_D1==='true'?await localD1(original.raw):null,DB=native||original,env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-agency-labels')};let cookie='';
 const rows=async(sql,...v)=>(await DB.prepare(sql).bind(...v).all()).results,run=(sql,...v)=>DB.prepare(sql).bind(...v).run();
 async function call(action,data={}){const r=await worker.fetch(new Request('http://localhost/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);const next=r.headers.getSetCookie().find(x=>x.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];return rememberOperation(await r.json(),cookie);}
 const ok=async(a,b={})=>{const r=await call(a,b);assert.equal(r.ok,true,a+': '+r.error);return r;};
 await ok('sop_login',{sop_key:'synthetic-agency-labels'});await confirmOperation(ok,'合成公司核验员');await ok('sop_attendance_config');
 const close=async()=>{await native?.close();original.raw.close();};t?.after(close);
 return {DB,env,worker,rows,run,call,ok,get cookie(){return cookie;},close};
}
