// Native local workerd D1; synthetic legacy issue relations, no remote account.
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
import {localD1} from './d1-local-runtime.mjs';
import {digest,issueAttachmentAllowed} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const DB=database(),bytes=new Uint8Array([137,80,78,71,0,255]);let reads=0,cookie='';
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-preview-d1'),R2_BUCKET:{async get(){reads++;return {body:bytes,httpMetadata:{contentType:'application/octet-stream'}};}}};
const api=async(action,data={})=>{const r=await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];const result=rememberOperation(await r.json(),cookie);assert.equal(result.ok,true,result.error);return result;};
await api('sop_login',{sop_key:'synthetic-preview-d1'});await confirmOperation(api,'Synthetic preview D1');
DB.raw.prepare("INSERT INTO v2_issue_tickets(id,biz_class) VALUES('ISS-PREVIEW','bulk')").run();
DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,issue_id,job_id) VALUES('RUN-PREVIEW','ISS-PREVIEW','JOB-PREVIEW')").run();
const aliases=[['issue_ticket','ISS-PREVIEW'],['issue_handle','RUN-PREVIEW'],['job','JOB-PREVIEW'],['ops_job','JOB-PREVIEW']];
for(const [type,id]of aliases)DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_key) VALUES(?,?,?,?)').run(type,type,id,'synthetic-preview/'+type);
const d1=await localD1(DB.raw);env.DB=d1;let checks=0;
try{
 const tables=['v2_issue_tickets','v2_issue_handle_runs','v2_attachments','sop_events'];
 const snapshot=async()=>{const all={};for(const name of tables)all[name]=(await d1.prepare('SELECT * FROM '+name+' ORDER BY rowid').all()).results;return all;};
 const before=await snapshot(),get=(path,c=cookie)=>worker.fetch(new Request('https://fixture.invalid'+path,{headers:{Cookie:c}}),env);
 for(const [type,id]of aliases){
  assert.equal(await issueAttachmentAllowed(env,{scope:'office'},type,id),true);
  assert.equal(await issueAttachmentAllowed(env,{scope:'field',departments:['bulk']},type,id),true);
  assert.equal(await issueAttachmentAllowed(env,{scope:'field',departments:['direct_ship']},type,id),false);
  assert.equal(await issueAttachmentAllowed(env,{scope:'kiosk'},type,id),false);checks++;
  for(const prefix of ['/api/file?key=','/file?key=','/api?action=v2_attachment_get&file_key=']){
   const r=await get(prefix+'synthetic-preview/'+type);assert.equal(r.status,200);assert.deepEqual(new Uint8Array(await r.arrayBuffer()),bytes);assert.equal(r.headers.get('Cache-Control'),'private, no-store');assert.equal(r.headers.get('X-Content-Type-Options'),'nosniff');checks++;
  }
 }
 const beforeDenied=reads;assert.equal((await get('/api/file?key=synthetic-preview/issue_ticket','')).status,401);assert.equal(reads,beforeDenied);checks++;
 await d1.prepare('DELETE FROM ck_access_sessions').run();assert.equal((await get('/api/file?key=synthetic-preview/issue_ticket')).status,401);assert.equal(reads,beforeDenied);checks++;
 assert.deepEqual(await snapshot(),before);checks++;
 assert.equal(checks,19);console.log('PASS native local D1: '+checks+' legacy alias/department/private bytes/expired session/nonmutation checks');
}finally{await d1.close();DB.raw.close();}
