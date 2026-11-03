import test from 'node:test';import assert from 'node:assert/strict';
import entry from '../worker-v2/staging-entry.js';import {STAGING_HOST,TABLES} from '../worker-v2/test-data-reset.js';
import {operationHeaders} from './office-operator-fixture.mjs';
import {resetLifecycleFixture} from './test-reset-lifecycle-fixture.mjs';import {holdMaterialBatch} from './batch-material-reference-fixture.mjs';
import {withResetFence} from '../worker-v2/test-reset-fence.js';
import {pathToFileURL} from 'node:url';
// An optional published writer isolates mixed-version rollout safety. The reset
// controller/DB always stay on this candidate; no remote request is made.
const writer=process.env.CK_RESET_INFLIGHT_WRITER_MODULE?(await import(pathToFileURL(process.env.CK_RESET_INFLIGHT_WRITER_MODULE))).default:entry;
const create=async(f,body)=>{const r=await writer.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,'Content-Type':'application/json',...operationHeaders(f.cookies.office)},body:JSON.stringify({action:'v2_inbound_plan_create',client_req_id:crypto.randomUUID(),customer:'合成在途入库',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:2}],...body})}),{...f.env});return {status:r.status,body:await r.json()};};
function holdLastZero(f){
 const db=f.env.DB;let reached,release,held=false;const ready=new Promise(r=>reached=r),gate=new Promise(r=>release=r);
 const prepare=sql=>{const wrap=s=>new Proxy(s,{get(target,key){if(key==='bind')return(...args)=>wrap(target.bind(...args));if(key==='first')return async(...args)=>{const result=await target.first(...args);if(!held&&sql.startsWith('SELECT (SELECT COUNT(*) FROM')&&sql.includes('v2_idempotency_keys')){held=true;reached();await gate;}return result;};const value=target[key];return typeof value==='function'?value.bind(target):value;}});return wrap(db.prepare(sql));};
 f.env.DB={prepare,batch:db.batch.bind(db)};return {ready,release,restore(){f.env.DB=db;}};
}
const remaining=async f=>{const names=(await f.rows("SELECT name FROM sqlite_master WHERE type='table'")).map(r=>r.name),result=[];for(const table of TABLES.filter(n=>names.includes(n))){const n=(await f.query('SELECT COUNT(*) n FROM '+table).first()).n;if(n)result.push({table,n});}return result;};
async function arrived(ready,pending){let timer;try{await Promise.race([ready,pending.then(result=>{throw Error('Expected transaction boundary was not reached: '+JSON.stringify(result));}),new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('Timed out waiting for synthetic transaction boundary')),15000))]);}finally{clearTimeout(timer);}}
for(const boundary of ['after-zero-check','after-completion'])test('an admitted inbound create cannot write '+boundary,async t=>{
 const f=await resetLifecycleFixture(t),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT INTO v2_inbound_plans'))),pending=create(f);await arrived(hold.ready,pending);let zero,finishing,result,done;
 try{
  if(boundary==='after-zero-check')zero=holdLastZero(f);
  const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});finishing=f.complete(run);
  if(zero)await zero.ready;else done=await finishing;
  hold.release();result=await pending;
 }finally{hold.release();zero?.release();}
 done||=await finishing;zero?.restore();hold.restore();
 const residual=await remaining(f);t.diagnostic(JSON.stringify({boundary,requestOk:result.body.ok,completed:!!done.run.completedAt,residual}));
 assert.equal(result.body.ok,false,'old admission must be refused, including after the active marker closes');assert.ok(done.run.completedAt);assert.deepEqual(residual,[],'completed reset must not conceal unarchived late business records');
});
test('cold identity during maintenance cannot bootstrap cleared attendance children',async t=>{
 const f=await resetLifecycleFixture(t),{run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});
 for(let i=0;i<50&&!await f.query("SELECT 1 FROM ck_test_reset_items WHERE run_id=? AND table_name='ck_attendance_spans'",run.id).first();i++)await f.okReset('step',{runId:run.id});
 assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);const db=f.env.DB,cold={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
 const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,'Content-Type':'application/json'},body:JSON.stringify({action:'sop_identity'})}),{...f.env,DB:cold}),identity=await response.json();
 assert.equal(identity.ok,true,JSON.stringify(identity));assert.equal(identity.user.role,'manager');assert.ok(identity.user.operation_context.name);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[],'identity must not recreate the first span from remaining parents');
 const done=await f.complete(run);assert.ok(done.run.completedAt);assert.deepEqual(await remaining(f),[]);
});
for(const [method,sql]of [
 ['run',"INSERT INTO v2_inbound_plans(id,customer) VALUES('SYN-LATE-RUN','late')"],
 ['all',"WITH incoming(id) AS (VALUES('SYN-LATE-CTE')) INSERT INTO v2_inbound_plans(id,customer) SELECT id,'late' FROM incoming RETURNING id"],
 ['first',"UPDATE v2_003_locations SET location_name='stale' WHERE id='SYN-RESET-KEEP' RETURNING id"],
 ['run',"DELETE FROM v2_003_locations WHERE id='SYN-RESET-KEEP'"]
])test('stale statement '+method+' is fenced: '+sql.split(' ')[0],async t=>{
 const f=await resetLifecycleFixture(t);let ready,release;const reached=new Promise(r=>ready=r),gate=new Promise(r=>release=r),pending=withResetFence(f.env,async e=>{ready();await gate;await e.DB.prepare(sql)[method]();return Response.json({ok:true});});await reached;
 try{const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});await f.complete(run);}finally{release();}
 const response=await pending;assert.equal(response.status,503);assert.deepEqual(await remaining(f),[]);assert.equal((await f.query("SELECT location_name FROM v2_003_locations WHERE id='SYN-RESET-KEEP'").first()).location_name,'合成保留库位');
});
test('a failed reset start rolls its generation back so the earlier business request can finish',async t=>{
 const f=await resetLifecycleFixture(t),generation=(await f.query('SELECT generation FROM ck_test_reset_epoch').first()).generation,hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT INTO v2_inbound_plans'))),pending=create(f);await hold.ready;
 await f.query("CREATE TRIGGER synthetic_reset_start_failure BEFORE INSERT ON ck_test_resets BEGIN SELECT RAISE(ABORT,'synthetic start failure'); END").run();let result;
 try{const failed=await f.reset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});assert.equal(failed.result.ok,false);assert.match(failed.result.error,/synthetic start failure/);assert.equal((await f.query('SELECT generation FROM ck_test_reset_epoch').first()).generation,generation);}finally{hold.release();result=await pending;hold.restore();}
 assert.equal(result.body.ok,true,JSON.stringify(result));assert.ok(await f.query('SELECT id FROM v2_inbound_plans WHERE id=?',result.body.id).first());await f.query('DROP TRIGGER synthetic_reset_start_failure').run();
});
test('a second DB binding can reset while the old binding holds a business request',async t=>{
 const f=await resetLifecycleFixture(t),db=f.env.DB,hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT INTO v2_inbound_plans'))),pending=create(f);await arrived(hold.ready,pending);const held=f.env.DB;
 f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};
 try{const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});await f.complete(run);}finally{hold.release();}
 const result=await pending;assert.equal(result.body.ok,false);assert.deepEqual(await remaining(f),[]);f.env.DB=held;hold.restore();
});
test('scheduled cutoff admitted before the reset cannot commit after it completes',async t=>{
 const f=await resetLifecycleFixture(t),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT OR IGNORE INTO ck_system_cutoff_audit'))),pending=f.worker.scheduled({scheduledTime:Date.now()+86400000},{...f.env});await hold.ready;
 try{const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});await f.complete(run);}finally{hold.release();}
 await pending;hold.restore();assert.deepEqual(await remaining(f),[]);
});
const multipart=async(f,category='batch_work_material')=>{const need=(await f.rows("SELECT id FROM sop_records WHERE kind='need' AND json_extract(state,'$.source_id')=?",f.plan.id))[0],form=new FormData();for(const [key,value]of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:f.plan.id,need_id:need.id,attachment_category:category,client_req_id:crypto.randomUUID()}))form.set(key,value);form.set('file',new File(['synthetic inflight object'],'合成在途资料.csv',{type:'text/csv'}));const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,...operationHeaders(f.cookies.office)},body:form}),{...f.env});return {status:response.status,body:await response.json()};};
test('reset waits for the entire admitted object upload and backs up its committed attachment',async t=>{
 const f=await resetLifecycleFixture(t),put=f.env.R2_BUCKET.put;let ready,release;const reached=new Promise(r=>ready=r),gate=new Promise(r=>release=r);f.env.R2_BUCKET.put=async(...args)=>{ready();await gate;return put(...args);};const pending=multipart(f);await reached;
 const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});let uploaded;
 try{const waiting=await f.reset('step',{runId:run.id});assert.equal(waiting.status,409);assert.match(waiting.result.error,/附件操作/);assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=?',run.id).first()).n,0);}finally{release();uploaded=await pending;}
 assert.equal(uploaded.body.ok,true,JSON.stringify(uploaded));assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_objects').first()).n,0);await f.complete(run);assert.ok(await f.query('SELECT * FROM ck_backup_'+run.id+'_v2_attachments WHERE id=?',uploaded.body.id).first());assert.equal(f.objects.size,2);assert.deepEqual(await remaining(f),[]);
});
test('an admitted object delete finishes its database delete before any reset backup',async t=>{
 const f=await resetLifecycleFixture(t),key='synthetic/inflight-delete',id='SYN-DELETE-ATTACHMENT';f.objects.set(key,{body:'synthetic'});await f.query("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key,file_name) VALUES(?,'inbound_plan',?,'arrival_photo',?,'synthetic.txt')",id,f.plan.id,key).run();let ready,release;const reached=new Promise(r=>ready=r),gate=new Promise(r=>release=r);f.env.R2_BUCKET.delete=async value=>{ready();await gate;f.objects.delete(value);};const pending=create(f,{action:'v2_attachment_delete',id});await arrived(reached,pending);
 const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});let deleted,waiting,earlyBackup;
 try{waiting=await f.reset('step',{runId:run.id});if(waiting.status!==409){await f.complete(run);earlyBackup=await f.query('SELECT file_key FROM ck_backup_'+run.id+'_v2_attachments WHERE id=?',id).first();}}finally{release();deleted=await pending;}
 t.diagnostic(JSON.stringify({drainStatus:waiting.status,deleteOk:deleted.body.ok,earlyBackupKey:earlyBackup?.file_key||'',objectStillPresent:f.objects.has(key)}));
 assert.equal(waiting.status,409,'no backup may begin while the object request can still delete a referenced blob');assert.equal(deleted.body.ok,true,JSON.stringify(deleted));assert.equal(await f.query('SELECT id FROM v2_attachments WHERE id=?',id).first(),null);await f.complete(run);assert.equal(await f.query('SELECT id FROM ck_backup_'+run.id+'_v2_attachments WHERE id=?',id).first(),null);assert.equal(f.objects.size,1);
});
test('an uncertain object result persists across restart and prevents any archival or clearing',async t=>{
 const f=await resetLifecycleFixture(t);f.env.R2_BUCKET.put=async()=>{throw Error('synthetic uncertain object result');};assert.equal((await multipart(f)).body.ok,false);
 assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_objects WHERE uncertain=1').first()).n,1);const before=await remaining(f),{run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'}),db=f.env.DB;
 f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};const waiting=await f.reset('step',{runId:run.id});assert.equal(waiting.status,409);assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=?',run.id).first()).n,0);assert.deepEqual(await remaining(f),before);assert.equal((await f.query('SELECT completed_at FROM ck_test_resets WHERE id=?',run.id).first()).completed_at,'');assert.equal(f.objects.size,1);
});
test('an old active run gets one durable generation on cold upgrade and still rejects earlier work',async t=>{
 const f=await resetLifecycleFixture(t),db=f.env.DB,hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INSERT INTO v2_inbound_plans'))),pending=create(f);await arrived(hold.ready,pending);
 const id=crypto.randomUUID().replaceAll('-',''),names=(await f.rows("SELECT name FROM sqlite_master WHERE type='table'")).map(r=>r.name),tables=TABLES.filter(n=>names.includes(n));await f.query('INSERT INTO ck_test_resets(id,request_id,actor,started_at,tables_json) VALUES(?,?,?,?,?)',id,crypto.randomUUID(),'synthetic-legacy',new Date().toISOString(),JSON.stringify(tables)).run();
 f.env.DB={prepare:db.prepare.bind(db),batch:db.batch.bind(db)};const run={id};
 try{await f.complete(run);assert.equal((await f.query('SELECT generation FROM ck_test_reset_epoch').first()).generation,1);}finally{hold.release();}
 assert.equal((await pending).body.ok,false);assert.deepEqual(await remaining(f),[]);hold.restore();
});
test('maintenance login, identity, operator confirmation and logout cannot initialize business data',async t=>{
 const f=await resetLifecycleFixture(t),{run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});for(let i=0;i<50&&!await f.query("SELECT 1 FROM ck_test_reset_items WHERE run_id=? AND table_name='ck_attendance_spans'",run.id).first();i++)await f.okReset('step',{runId:run.id});
 const db=f.env.DB,cold=()=>({prepare:db.prepare.bind(db),batch:db.batch.bind(db)}),before=await remaining(f);
 const auth=async(action,extra={},context='')=>{const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api',{method:'POST',headers:{Cookie:f.cookies.office,'Content-Type':'application/json',...(context?{'X-CK-Operation-Context':context}:{})},body:JSON.stringify({action,...extra})}),{...f.env,DB:cold()});const cookie=response.headers.getSetCookie().find(s=>s.startsWith('ck_office_access='));if(cookie)f.cookies.office=cookie.split(';')[0];return response.json();};
 assert.equal((await auth('sop_login',{sop_key:'synthetic-owned-home-fixture'})).ok,true);const identity=await auth('sop_identity');assert.equal(identity.user.role,'manager');assert.equal((await auth('sop_operator_confirm',{name:'合成维护负责人'},identity.user.operation_context.id)).ok,true);assert.equal((await auth('sop_logout')).ok,true);assert.equal((await auth('sop_login',{sop_key:'synthetic-owned-home-fixture'})).ok,true);
 assert.deepEqual(await remaining(f),before);assert.deepEqual(await f.rows('SELECT * FROM ck_attendance_spans'),[]);await f.complete(run);assert.deepEqual(await remaining(f),[]);
});
test('object success followed by database failure stays unverified and cannot be cleared away',async t=>{
 const f=await resetLifecycleFixture(t);await f.query("CREATE TRIGGER synthetic_attachment_commit_failure BEFORE INSERT ON v2_attachments BEGIN SELECT RAISE(ABORT,'synthetic attachment commit failure'); END").run();const result=await multipart(f);assert.equal(result.body.ok,false);assert.equal(f.objects.size,2,'retain the successfully written object for verification');assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_objects WHERE uncertain=1').first()).n,1);
 const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});assert.equal((await f.reset('step',{runId:run.id})).status,409);assert.equal((await f.query('SELECT COUNT(*) n FROM ck_test_reset_items WHERE run_id=?',run.id).first()).n,0);
});
test('a field task completion cannot publish output after the reset',async t=>{
 const f=await resetLifecycleFixture(t),hold=holdMaterialBatch(f,ss=>ss.some(s=>s.sql.includes('INTO v2_ops_job_results'))),body={action:'sop_task_complete_review',client_req_id:crypto.randomUUID(),id:f.second.id,revision:f.record(f.second.id).revision,result:{quantity:2,unit:'箱',label_count:2},decision:'pass',reason:'合成在途完成'},pending=entry.fetch(new Request('https://'+STAGING_HOST+'/001/api',{method:'POST',headers:{Cookie:f.cookies.A,'Content-Type':'application/json'},body:JSON.stringify(body)}),{...f.env});await arrived(hold.ready,pending.then(r=>r.clone().json()));
 try{const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});await f.complete(run);}finally{hold.release();}
 const result=await(await pending).json();hold.restore();assert.equal(result.ok,false);assert.deepEqual(await remaining(f),[]);
});
test('an admitted kiosk checkin cannot rebuild people or attendance after the reset',async t=>{
 const f=await resetLifecycleFixture(t),login=await entry.fetch(new Request('https://'+STAGING_HOST+'/attendance/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'sop_login',sop_key:'synthetic-owned-home-fixture'})}),f.env),loginBody=await login.json();assert.equal(loginBody.ok,true,JSON.stringify(loginBody));const cookie=login.headers.getSetCookie().find(s=>s.startsWith('ck_kiosk_access=')).split(';')[0];
 const hold=holdMaterialBatch(f,ss=>ss.some(s=>/INSERT(?: OR IGNORE)? INTO ck_attendance_days/.test(s.sql))),pending=entry.fetch(new Request('https://'+STAGING_HOST+'/attendance/api',{method:'POST',headers:{Cookie:cookie,'Content-Type':'application/json'},body:JSON.stringify({action:'sop_attendance_checkin',client_req_id:crypto.randomUUID(),name:'合成在途签到',agency:'가온'})}),{...f.env});await arrived(hold.ready,pending.then(r=>r.clone().json()));
 try{const {run}=await f.okReset('start',{requestId:crypto.randomUUID(),confirmation:'CLEAR_TEST_DATA'});await f.complete(run);}finally{hold.release();}
 const result=await(await pending).json();hold.restore();assert.equal(result.ok,false);assert.deepEqual(await remaining(f),[]);
});
