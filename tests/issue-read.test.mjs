import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import worker from '../worker-v2/index.js';
import {digest,issueAttachmentAllowed} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const stamp='2026-10-01T01:00:00Z';
async function fixture(){const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};const api=async b=>(await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)}),env)).json();await api({action:'sop_identity'});return{DB,env,api};}
test('summary paging is stable, filters search full descriptions, and details/legacy responses retain full text',async()=>{
 const {DB,api}=await fixture(),description='Short title '+ 'long history '.repeat(1000)+'search-tail';
 for(let i=0;i<88;i++)DB.raw.prepare('INSERT INTO v2_issue_tickets(id,biz_class,customer,issue_description,issue_summary,status,created_at,accounting_required,accounted,latest_feedback_text) VALUES(?,?,?,?,?,?,?,?,?,?)').run('ISS-'+String(i).padStart(3,'0'),'bulk','Synthetic '+i,description,'Summary','responded',stamp,1,i%2,'Full feedback');
 const request={action:'v2_issue_list',list_summary:true,limit:50};const a=await api(request),b=await api({...request,offset:50});assert.equal(a.total,88);assert.equal(a.items.length,50);assert.equal(b.items.length,38);assert.equal(new Set([...a.items,...b.items].map(x=>x.id)).size,88);assert.equal(a.items[0].id,'ISS-087');assert.equal((await api({...request,sort:'oldest_first'})).items[0].id,'ISS-000');
 assert.equal(a.items[0].issue_description.length,512);assert.equal(a.items[0].latest_feedback_text,undefined);
 assert.equal((await api({...request,related_doc_q:'search-tail',accounted:0})).total,44);
 assert.equal((await api({...request,status:'pending'})).total,0);assert.equal((await api({...request,customer_q:'Synthetic 87',biz_class:'bulk'})).total,1);
 const legacy=await api({...request,list_summary:false});assert.equal(legacy.items[0].issue_description,description);assert.equal(legacy.items[0].latest_feedback_text,'Full feedback');assert.equal((await api({action:'v2_issue_detail',id:'ISS-087'})).issue.issue_description,description);
});
test('detail history uses two DB calls with more than 100 job IDs, retains legacy attachments and segment accounting without writes',async()=>{
 const {DB,api}=await fixture();DB.raw.prepare('INSERT INTO v2_issue_tickets(id,biz_class,created_at) VALUES(?,?,?)').run('ISS-X','bulk',stamp);
 for(let i=0;i<120;i++){DB.raw.prepare('INSERT INTO v2_issue_handle_runs(id,issue_id,job_id,run_status,started_at,ended_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run('RUN-'+i,'ISS-X','JOB-'+i,'completed',stamp,'2026-10-01T02:00:00Z',7);
  for(const [j,left,minutes]of [[0,'2026-10-01T01:10:00Z',0],[1,'',0],[2,'2026-10-01T01:30:00Z',5]])DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run('S'+i+'-'+j,'JOB-'+i,'W'+j,'Synthetic '+j,stamp,left,minutes);
 }
 for(const [i,type,id]of [[0,'issue_ticket','ISS-X'],[1,'issue','ISS-X'],[2,'issue_handle','RUN-0'],[3,'issue_handle_run','RUN-0'],[4,'ops_job','JOB-0'],[5,'job','JOB-0'],[6,'issue_ticket','JOB-0']])DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_key,created_at) VALUES(?,?,?,?,?)').run('A'+i,type,id,'synthetic/'+i,stamp);
 const prepare=DB.prepare,batch=DB.batch;let calls=0;DB.prepare=sql=>{const s=prepare(sql);for(const m of ['first','all','run']){const f=s[m];s[m]=async()=>{calls++;assert.doesNotMatch(sql,/^\s*(INSERT|UPDATE|DELETE)/);return f();};}return s;};DB.batch=async ss=>{calls++;for(const s of ss)assert.doesNotMatch(s.sql,/^\s*(INSERT|UPDATE|DELETE)/);return batch(ss);};
 const d=await api({action:'v2_issue_detail',id:'ISS-X'});assert.equal(calls,2);assert.equal(d.handle_runs.length,120);assert.equal(d.attachments.length,2);const r=d.handle_runs[0];assert.equal(r.attachments.length,5);assert.equal(r.actual_minutes,75);assert.equal(r.natural_span_minutes,60);assert.equal(r.segments[1].open,true);assert.equal(r.segments[1].left_at,'');assert.equal(DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE left_at='' ").get().n,120);
 assert.equal((await api({action:'v2_issue_detail',id:'MISSING'})).ok,false);
});
test('legacy run relation resolves only a real issue and preserves department boundaries',async()=>{
 const {DB,env}=await fixture();DB.raw.prepare("INSERT INTO v2_issue_tickets(id,biz_class) VALUES('ISS-X','bulk')").run();DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,issue_id,job_id) VALUES('RUN-X','ISS-X','JOB-X')").run();
 for(const [type,id]of [['issue_ticket','ISS-X'],['issue','ISS-X'],['issue_handle','RUN-X'],['issue_handle_run','RUN-X'],['issue_ticket','JOB-X']]){
  assert.equal(await issueAttachmentAllowed(env,{scope:'office'},type,id),true);assert.equal(await issueAttachmentAllowed(env,{scope:'field',departments:['bulk']},type,id),true);assert.equal(await issueAttachmentAllowed(env,{scope:'field',departments:['direct_ship']},type,id),false);assert.equal(await issueAttachmentAllowed(env,{scope:'kiosk'},type,id),false);
 }assert.equal(await issueAttachmentAllowed(env,{scope:'office'},'issue_handle','unknown'),false);
});
test('existing admin dry-run still uses the original per-job helper without modifying records',async()=>{
 const {DB,env,api}=await fixture();env.ADMINKEY='synthetic-read-admin';
 DB.raw.prepare("INSERT INTO v2_issue_tickets(id,biz_class) VALUES('ISS-X','bulk')").run();
 DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,issue_id,job_id,run_status,started_at,ended_at,minutes_worked) VALUES('RUN-X','ISS-X','JOB-X','completed',?,'2026-10-01T02:00:00Z',3)").run(stamp);
 DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES('W','JOB-X','W','Synthetic',?,'2026-10-01T02:00:00Z',60)").run(stamp);
 const result=await api({action:'v2_admin_recalc_issue_work_minutes',issue_id:'ISS-X',dry_run:true,k:'synthetic-read-admin'});assert.equal(result.ok,true,result.error);assert.equal(result.diffs[0].new_minutes,60);assert.equal(result.diffs[0].old_minutes,3);assert.equal(DB.raw.prepare("SELECT minutes_worked FROM v2_issue_handle_runs WHERE id='RUN-X'").get().minutes_worked,3);
});
test('real private GET aliases authorize historical originals, reject expired/foreign sessions, and return exact original bytes',async()=>{
 const {DB,env}=await fixture();delete env.SOP_PUBLIC_TEST_ACCESS;Object.assign(env,{SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-legacy')});const bytes=new Uint8Array([137,80,78,71,0,255]);let reads=0;env.R2_BUCKET={async get(){reads++;return {body:bytes,httpMetadata:{contentType:'application/octet-stream'}};}};
 let cookie='';const api=async(action,b={})=>{const r=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,...b,client_req_id:crypto.randomUUID()})}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];return rememberOperation(await r.json(),cookie);};await api('sop_login',{sop_key:'synthetic-legacy'});await confirmOperation(api);
 DB.raw.prepare("INSERT INTO v2_issue_tickets(id,biz_class) VALUES('ISS-X','bulk')").run();DB.raw.prepare("INSERT INTO v2_issue_handle_runs(id,issue_id,job_id) VALUES('RUN-X','ISS-X','JOB-X')").run();
 for(const type of ['issue_handle','job','ops_job']){const id=type==='issue_handle'?'RUN-X':'JOB-X';DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_key) VALUES(?,?,?,?)').run(type,type,id,'synthetic/'+type);}
 const get=(path,c=cookie)=>worker.fetch(new Request('https://fixture.test'+path,{headers:{Cookie:c}}),env);
 for(const type of ['issue_handle','job','ops_job'])for(const url of ['/api/file?key=','/file?key=','/api?action=v2_attachment_get&file_key=']){const r=await get(url+'synthetic/'+type);assert.equal(r.status,200);assert.deepEqual(new Uint8Array(await r.arrayBuffer()),bytes);assert.equal(r.headers.get('Cache-Control'),'private, no-store');assert.equal(r.headers.get('X-Content-Type-Options'),'nosniff');}
 const before=reads;assert.equal((await get('/api/file?key=synthetic/issue_handle','')).status,401);assert.equal(reads,before);
 DB.raw.prepare("UPDATE ck_access_sessions SET scope='field',departments=?").run(JSON.stringify(['direct_ship']));
 for(const type of ['issue_handle','job','ops_job'])assert.equal((await get('/001/api/file?key=synthetic/'+type)).status,401);assert.equal(reads,before);
 DB.raw.prepare('DELETE FROM ck_access_sessions').run();assert.equal((await get('/api/file?key=synthetic/issue_handle')).status,401);
});
