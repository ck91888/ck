import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {uploadIssueDraft,createIssueWithAttachments,cancelIssueDraft,removeIssueDraftFile} from '../worker-v2/issue-create.js';
import worker from '../worker-v2/index.js';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
import {png,pdf,xlsx,xls,bytesFor} from './issue-attachment-fixtures.mjs';
function fixture(){
 const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_REQUEST_USER:{id:'OFFICE',name:'Synthetic office',scope:'office',role:'manager'},SOP_OPERATION_SESSION:'SESSION-A',R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:new Uint8Array(await new Response(body).arrayBuffer()),...meta});},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}}};
 const draft='ISD-'+crypto.randomUUID(),files=[];
 const form=(id='ISF-'+crypto.randomUUID(),name='synthetic.png',bytes=png,d=draft)=>{const f=new FormData();f.set('related_doc_type','issue_draft');f.set('related_doc_id',d);f.set('client_req_id',id);f.set('file',new File([bytes],name));return f;};
 const upload=async(name='synthetic.png',bytes=bytesFor(name))=>{const f=form(undefined,name,bytes),r=await uploadIssueDraft(f,env);files.push(r.id);return f;};
 const body=()=>({biz_class:'bulk',customer:'Synthetic customer',issue_description:'Synthetic issue',issue_attachments:{draft_id:draft,attachment_ids:[...files]}});
 const count=table=>DB.raw.prepare('SELECT count(*) n FROM '+table).get().n;
 return {DB,env,objects,draft,files,form,upload,body,count};
}
test('two photos, PDF and Excel stay private until one atomic ticket creation; retries create one ticket',async()=>{
 const s=fixture();for(const name of ['one.png','two.png','manual.pdf','details.xlsx'])await s.upload(name);
 assert.equal(s.count('v2_issue_tickets'),0);assert.equal(s.count('v2_attachments'),0);
 const [a,b]=await Promise.all([createIssueWithAttachments(s.body(),s.env),createIssueWithAttachments(s.body(),s.env)]);
 assert.equal(a.id,b.id);assert.equal(s.count('v2_issue_tickets'),1);assert.equal(s.count('v2_attachments'),4);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_attachments WHERE related_doc_type=? AND related_doc_id=?').get('issue_ticket',a.id).n,4);
 await cancelIssueDraft({draft_id:s.draft},s.env);assert.equal(s.objects.size,4);
 await assert.rejects(()=>createIssueWithAttachments({...s.body(),issue_description:'Changed after commit'},s.env),/已提交/);
});
test('R2 failure and lost upload response can retry original file without duplicates',async()=>{
 const s=fixture(),f=s.form(),put=s.env.R2_BUCKET.put;s.env.R2_BUCKET.put=async()=>{throw Error('synthetic offline');};
 await assert.rejects(()=>uploadIssueDraft(f,s.env),/offline/);assert.equal(s.count('v2_attachments'),0);assert.equal(s.count('v2_issue_tickets'),0);
 s.env.R2_BUCKET.put=put;const a=await uploadIssueDraft(f,s.env),b=await uploadIssueDraft(f,s.env);assert.equal(a.id,b.id);assert.equal(s.objects.size,1);
 await assert.rejects(()=>uploadIssueDraft(s.form(a.id,'renamed.png',png),s.env),/已变化/);
});
test('failed transaction publishes neither ticket nor attachment and corrected retry succeeds',async()=>{
 const s=fixture();await s.upload();const batch=s.env.DB.batch;s.env.DB.batch=async()=>{throw Error('synthetic transaction failure');};
 await assert.rejects(()=>createIssueWithAttachments(s.body(),s.env),/transaction/);assert.equal(s.count('v2_issue_tickets'),0);assert.equal(s.count('v2_attachments'),0);
 s.env.DB.batch=batch;assert.equal((await createIssueWithAttachments(s.body(),s.env)).ok,true);
});
test('cross-draft and cross-office-session attachment claims are refused; selected subset only',async()=>{
 const s=fixture();await s.upload();const a=s.files[0];await s.upload('removed.png');
 const foreign=s.body();foreign.issue_attachments.draft_id='ISD-'+crypto.randomUUID();await assert.rejects(()=>createIssueWithAttachments(foreign,s.env),/不属于/);
 s.env.SOP_OPERATION_SESSION='SESSION-B';await assert.rejects(()=>createIssueWithAttachments(s.body(),s.env),/本次登录/);s.env.SOP_OPERATION_SESSION='SESSION-A';
 const selected=s.body();selected.issue_attachments.attachment_ids=[a];await createIssueWithAttachments(selected,s.env);assert.equal(s.count('v2_attachments'),1);assert.equal(s.objects.size,1);
});
test('cancellation cannot be reversed by a late upload or subsequent create',async()=>{
 const s=fixture(),f=s.form();let release,entered;const pending=new Promise(r=>entered=r),put=s.env.R2_BUCKET.put;s.env.R2_BUCKET.put=async(...args)=>{entered();await new Promise(r=>release=r);await put(...args);};
 const upload=uploadIssueDraft(f,s.env);await pending;await cancelIssueDraft({draft_id:s.draft},s.env);release();await assert.rejects(()=>upload,/取消/);
 await assert.rejects(()=>createIssueWithAttachments(s.body(),s.env),/取消/);assert.equal(s.count('v2_issue_tickets'),0);assert.equal(s.count('v2_attachments'),0);
});
test('file limits and office role are enforced server-side',async()=>{
 const s=fixture();for(const [name,bytes] of [['script.html','x'],['empty.pdf',''],['large.pdf',new Uint8Array(20*1024*1024+1)]])await assert.rejects(()=>uploadIssueDraft(s.form(undefined,name,bytes),s.env),/20MB/);
 s.env.SOP_REQUEST_USER.scope='field';await assert.rejects(()=>uploadIssueDraft(s.form(),s.env),/办公室/);s.env.SOP_REQUEST_USER.scope='office';s.env.SOP_REQUEST_USER.role='viewer';await assert.rejects(()=>uploadIssueDraft(s.form(),s.env),/办公室/);
});
test('real Worker authorizes originals in office and matching field department; blocks drafts, guests and foreign department',async()=>{
 const s=fixture();Object.assign(s.env,{SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-issue-files')});
 let cookie='';const call=async(action,b={},url='/api')=>{const r=await worker.fetch(new Request('https://test.local'+url,{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),s.env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookie);};
 assert.equal((await call('sop_login',{sop_key:'synthetic-issue-files'})).ok,true);await confirmOperation(call);const office=cookie;
 const bulk=(await call('sop_attendance_employee_register',{employeeNo:'ISS-AT-B',name:'Synthetic bulk',department:'bulk'})).person;
 const direct=(await call('sop_attendance_employee_register',{employeeNo:'ISS-AT-D',name:'Synthetic direct',department:'direct_ship'})).person;
 for(const p of [bulk,direct])assert.equal((await call('sop_attendance_checkin',{badge:p.badgeId})).ok,true);
 const f=s.form(),r=await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie)},body:f}),s.env);assert.equal((await r.json()).ok,true);s.files.push(f.get('client_req_id'));
 const file=JSON.parse(s.DB.raw.prepare("SELECT state FROM sop_records WHERE kind='issue_file'").get().state);
 const forged=s.form();forged.set('related_doc_type','issue_ticket');forged.set('attachment_category','issue_original');forged.set('related_doc_id','ISS-forged');assert.equal((await worker.fetch(new Request('https://test.local/api',{method:'POST',headers:{Cookie:cookie,...operationHeaders(cookie)},body:forged}),s.env)).status,403);
 const get=async(c,url='/api/file')=>worker.fetch(new Request('https://test.local'+url+'?key='+encodeURIComponent(file.file_key),{headers:{Cookie:c}}),s.env);
 assert.equal((await get(office)).status,401);assert.equal((await call('v2_attachment_get',{file_key:file.file_key})).ok,false);
 const created=await call('v2_issue_create',s.body());assert.equal(created.ok,true,created.error);assert.equal((await get(office)).status,200);
 const details=await call('v2_issue_detail',{id:created.id});assert.equal(details.attachments.length,1);
 cookie='';await call('sop_login',{badge:bulk.badgeId},'/001/api');const field=cookie;assert.equal((await call('v2_issue_detail',{id:created.id},'/001/api')).ok,true);const download=await get(field,'/001/api/file');assert.equal(download.status,200);assert.equal(download.headers.get('Cache-Control'),'private, no-store');assert.equal(download.headers.get('X-Content-Type-Options'),'nosniff');
 cookie='';await call('sop_login',{badge:direct.badgeId},'/001/api');
 // Scope a synthetic session only; normal field login's established all-department grant stays unchanged.
 s.DB.raw.prepare("UPDATE ck_access_sessions SET departments=? WHERE token_hash=?").run(JSON.stringify(['direct_ship']),await digest(cookie.split('=')[1]));
 assert.equal((await get(cookie,'/001/api/file')).status,401);assert.equal((await call('v2_issue_detail',{id:created.id},'/001/api')).ok,false);assert.equal((await call('v2_attachment_list',{related_doc_type:'issue_ticket',related_doc_id:created.id},'/001/api')).ok,false);assert.equal((await get('')).status,401);
});

test('content signatures, document structure and MIME are checked, not just extensions',async()=>{
 const s=fixture();for(const name of ['forged.png','forged.jpg','forged.webp','forged.pdf','forged.xls','forged.xlsx','forged.csv'])await assert.rejects(()=>uploadIssueDraft(s.form(undefined,name,Buffer.from('<script>alert(1)</script>\x00')),s.env),/内容/);
 const mismatch=s.form(undefined,'valid.pdf',pdf);mismatch.set('file',new File([pdf],'valid.pdf',{type:'image/png'}));await assert.rejects(()=>uploadIssueDraft(mismatch,s.env),/类型/);
 const unrelatedZip=Buffer.from(xlsx);const workbook=unrelatedZip.indexOf('xl/workbook.xml');unrelatedZip.write('xx/workbook.xml',workbook);await assert.rejects(()=>uploadIssueDraft(s.form(undefined,'not-workbook.xlsx',unrelatedZip),s.env),/内容/);
 for(const [name,bytes] of [['real.pdf',pdf],['real.xls',xls],['real.xlsx',xlsx],['real.csv',Buffer.from('名称,数量\nSynthetic,2\n')]])assert.equal((await uploadIssueDraft(s.form(undefined,name,bytes),s.env)).ok,true);
 assert.equal(s.objects.size,4);
});
test('forged object URLs and files belonging to a committed different issue cannot be bound',async()=>{
 const s=fixture();await s.upload();const original=s.body(),first=await createIssueWithAttachments(original,s.env);
 const other={...original,issue_attachments:{draft_id:'ISD-'+crypto.randomUUID(),attachment_ids:s.files,file_key:'https://foreign.invalid/object'}};
 await assert.rejects(()=>createIssueWithAttachments(other,s.env),/不属于/);
 other.issue_attachments.attachment_ids=['https://foreign.invalid/object'];await assert.rejects(()=>createIssueWithAttachments(other,s.env),/编号无效/);
 assert.equal(s.count('v2_issue_tickets'),1);assert.equal(s.DB.raw.prepare('SELECT related_doc_id FROM v2_attachments').get().related_doc_id,first.id);
});
test('selected attachment count is checked server-side',async()=>{
 const s=fixture(),body=s.body();body.issue_attachments.attachment_ids=Array.from({length:21},()=>'ISF-'+crypto.randomUUID());await assert.rejects(()=>createIssueWithAttachments(body,s.env),/清单/);assert.equal(s.count('v2_issue_tickets'),0);
});
test('removing an uploaded file releases capacity and cannot remove another draft or published file',async()=>{
 const s=fixture();for(let i=0;i<20;i++)await s.upload('file-'+i+'.png');await assert.rejects(()=>s.upload('limit.png'),/20个/);
 const id=s.files.shift();await assert.rejects(()=>removeIssueDraftFile({draft_id:'ISD-'+crypto.randomUUID(),file_id:id},s.env),/不属于/);
 await removeIssueDraftFile({draft_id:s.draft,file_id:id},s.env);await removeIssueDraftFile({draft_id:s.draft,file_id:id},s.env);assert.equal(s.objects.size,19);await s.upload('replacement.png');await createIssueWithAttachments(s.body(),s.env);assert.equal(s.count('v2_attachments'),20);await assert.rejects(()=>removeIssueDraftFile({draft_id:s.draft,file_id:s.files[0]},s.env),/已提交/);assert.equal(s.objects.size,20);
});
