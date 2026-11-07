// Same synthetic fixture for retained baseline and candidate. No live data/access.
import {database} from './d1-adapter.mjs';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {gzipSync} from 'node:zlib';
import {createHash} from 'node:crypto';
const root=process.env.CK_ISSUE_SOURCE||process.cwd();
const {default:worker}=await import(pathToFileURL(resolve(root,'worker-v2/index.js')));
const {issueAttachmentAllowed}=await import(pathToFileURL(resolve(root,'worker-v2/access-control.js')));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
const api=async action=>{const r=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(action)}),env);return r.text();};
await api({action:'sop_identity'});
const stamp='2026-10-01T01:00:00Z';
for(let i=0;i<88;i++)DB.raw.prepare('INSERT INTO v2_issue_tickets(id,biz_class,customer,issue_description,latest_feedback_text,rework_note,status,created_at) VALUES(?,?,?,?,?,?,?,?)').run('ISS-'+i,'bulk','Synthetic customer '+i,'Synthetic issue '+i+' '+Array.from({length:180},(_,j)=>createHash('sha256').update(i+':'+j).digest('hex')).join(' '),'Synthetic feedback '.repeat(400),'Synthetic note '.repeat(300),'pending',stamp);
for(let i=0;i<40;i++){
 DB.raw.prepare('INSERT INTO v2_issue_handle_runs(id,issue_id,job_id,run_status,started_at,ended_at,handler_name) VALUES(?,?,?,?,?,?,?)').run('RUN-'+i,'ISS-0','JOB-'+i,'completed',stamp,'2026-10-01T02:00:00Z','Synthetic');
 for(let j=0;j<2;j++)DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run('SEG-'+i+'-'+j,'JOB-'+i,'W'+j,'Synthetic '+j,stamp,'2026-10-01T02:00:00Z',60);
}
for(let i=0;i<20;i++)DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,file_key,file_name,file_size,content_type) VALUES(?,?,?,?,?,?,?)').run('A'+i,'issue_ticket','ISS-0','synthetic/'+i+'.png',i+'.png',4*1024*1024,'image/png');
let calls=0,sqls=[],delay=0;const prepare=DB.prepare,batch=DB.batch;
const wait=async()=>{calls++;if(delay)await new Promise(r=>setTimeout(r,delay));};
DB.prepare=sql=>{const s=prepare(sql);for(const method of ['first','all','run']){const fn=s[method];s[method]=async()=>{sqls.push(sql);await wait();return fn();};}return s;};
DB.batch=async ss=>{sqls.push(...ss.map(s=>s.sql));await wait();return batch(ss);};
const measurements=[];
for(const action of [{action:'v2_issue_list',limit:50,offset:0,list_summary:true},{action:'v2_issue_detail',id:'ISS-0'}]){
 for(const ms of [0,20]){delay=ms;calls=0;sqls=[];const start=performance.now(),body=await api(action),value=JSON.parse(body);if(!value.ok)throw Error(body);measurements.push({action:action.action,injectedMs:ms,ms:Math.round(performance.now()-start),calls,statements:sqls.length,bytes:Buffer.byteLength(body),gzip:gzipSync(body).length,rows:value.items?.length,runs:value.handle_runs?.length,detailHash:action.id?createHash('sha256').update(body).digest('hex'):undefined});}
}
delay=0;
console.log(JSON.stringify({source:root,measurements,legacyRunAllowed:await issueAttachmentAllowed(env,{scope:'office'},'issue_handle','RUN-0')},null,2));
