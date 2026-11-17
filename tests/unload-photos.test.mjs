import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';
const png=()=>new File([Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aB9sAAAAASUVORK5CYII=','base64')],'arrival.png',{type:'image/png'});
async function setup(){
 const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'Fixture dispatcher',role:'manager',key:'fixture'},{id:'D',name:'Other dispatcher',role:'dispatcher',departments:['bulk'],key:'other'}]),R2_BUCKET:{async put(key,body,meta){objects.set(key,{body,httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}}};let cookie='';
 async function raw(body){const multi=body instanceof FormData;const r=await worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{Cookie:cookie,...(multi?{}:{'Content-Type':'application/json'})},body:multi?body:JSON.stringify(body)}),env);if(r.headers.has('set-cookie'))cookie=r.headers.get('set-cookie').split(';')[0];return r.json();}
 const call=(action,b={})=>raw({action,client_req_id:crypto.randomUUID(),...b});
 const ok=async(action,b={})=>{const r=await call(action,b);assert.equal(r.ok,true,r.error);return r;};
 const login=(key='fixture')=>ok('sop_login',{sop_key:key});await login();
 const plan=(customer='Fixture customer')=>ok('v2_inbound_plan_create',{customer,biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:10}]});
 const start=(ids)=>ok('sop_native_start',{payload:{action:ids?'v2_unload_job_start':'v2_unplanned_unload_start',...(ids?{plan_ids:ids}:{cargo_summary:'Fixture cargo'}),client_req_id:crypto.randomUUID()},workers:[{id:'A',name:'Fixture crew'}],lead_id:'A',estimated_minutes:10});
 const upload=(jobId,target,type='inbound_plan',extra={},file=png())=>{const form=new FormData();for(const [k,v] of Object.entries({action:'v2_attachment_upload',attachment_category:'unload_photo',job_id:jobId,related_doc_type:type,related_doc_id:target,client_req_id:crypto.randomUUID(),uploaded_by:'Spoofed uploader',...extra}))form.set(k,v);form.set('file',file);return raw(form);};
 const finish=async jobId=>{const d=await ok('v2_ops_job_detail',{job_id:jobId});return ok('v2_unload_job_finish',{job_id:jobId,complete_job:true,worker_id:'A',plan_results:d.unload_plans.map(({plan,lines})=>({plan_id:plan.id,lines:lines.map(l=>({line_id:l.id,actual_qty:l.planned_qty}))}))});};
 return {DB,env,objects,call,ok,login,plan,start,upload,finish};
}
test('truck photos are isolated per plan, persist after finish, and record the authenticated uploader',async()=>{
 const s=await setup(),a=await s.plan('A'),b=await s.plan('B'),j=await s.start([a.id,b.id]);
 const x=await s.upload(j.job_id,a.id),y=await s.upload(j.job_id,b.id);assert.equal(x.ok,true,x.error);assert.equal(y.ok,true,y.error);assert.equal(x.attachment.uploaded_by,'Fixture dispatcher');
 await s.finish(j.job_id);
 for(const [p,photo] of [[a,x],[b,y]]){const d=await s.ok('v2_inbound_plan_detail',{id:p.id});assert.equal(d.arrival_photos.length,1);assert.equal(d.arrival_photos[0].id,photo.id);assert.equal(d.plan.status,'completed');}
 const extra=await s.upload(j.job_id,a.id);assert.equal(extra.ok,true,extra.error);assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_ops_jobs').get().n,1);
});
test('retry returns the same photo, rejects request reuse and enforces file limits without R2 debris',async()=>{
 const s=await setup(),a=await s.plan(),j=await s.start([a.id]),client_req_id=crypto.randomUUID();const first=await s.upload(j.job_id,a.id,'inbound_plan',{client_req_id});assert.equal(first.ok,true,first.error);
 const repeat=await s.upload(j.job_id,a.id,'inbound_plan',{client_req_id});assert.equal(repeat.id,first.id);assert.equal(repeat.already_uploaded,true);assert.equal(s.objects.size,1);
 const changed=new File([await png().arrayBuffer(),'changed'],'arrival.png',{type:'image/png'});assert.equal((await s.upload(j.job_id,a.id,'inbound_plan',{client_req_id},changed)).ok,false);
 for(const file of [new File(['bad'],'fake.png',{type:'image/png'}),new File(['<svg/>'],'a.svg',{type:'image/svg+xml'}),new File([new Uint8Array(10*1024*1024+1)],'big.jpg',{type:'image/jpeg'})])assert.equal((await s.upload(j.job_id,a.id,'inbound_plan',{},file)).ok,false);
 assert.equal(s.objects.size,1);
 for(let i=1;i<12;i++)assert.equal((await s.upload(j.job_id,a.id)).ok,true);
 assert.equal((await s.upload(j.job_id,a.id)).ok,false);assert.equal(s.objects.size,12);
});
test('uploads cannot target another truck, impersonate a dispatcher, or bypass cancelled jobs',async()=>{
 const s=await setup(),a=await s.plan(),b=await s.plan('Unrelated'),j=await s.start([a.id]);
 assert.equal((await s.upload(j.job_id,b.id)).ok,false);
 await s.login('other');assert.equal((await s.upload(j.job_id,a.id)).ok,false);
 await s.login();s.DB.raw.prepare("UPDATE v2_ops_jobs SET status='cancelled' WHERE id=?").run(j.job_id);assert.equal((await s.upload(j.job_id,a.id)).ok,false);
 assert.equal(s.objects.size,0);
});
test('temporary arrival photos remain available after linking feedback to an existing plan',async()=>{
 const s=await setup(),p=await s.plan(),j=await s.start(),d=await s.ok('v2_ops_job_detail',{job_id:j.job_id}),id=d.job.related_doc_id;
 const photo=await s.upload(j.job_id,id,'field_feedback');assert.equal(photo.ok,true,photo.error);
 await s.ok('v2_unplanned_unload_finish',{job_id:j.job_id,worker_id:'A',complete_job:true,result_lines:[{unit_type:'carton',actual_qty:10}]});
 const preview=await s.ok('sop_feedback_link_preview',{feedback_id:id,plan_id:p.id});
 await s.ok('sop_feedback_link_save',{feedback_id:id,plan_id:p.id,feedback_version:preview.feedback_version,plan_version:preview.plan_version,job_version:preview.job_version,lines:preview.lines.map(l=>({line_id:l.id,unit_type:l.unit_type,actual_qty:l.actual_qty}))});
 assert.equal((await s.ok('v2_feedback_detail',{id})).arrival_photos[0].id,photo.id);
 assert.equal((await s.ok('v2_inbound_plan_detail',{id:p.id})).arrival_photos[0].id,photo.id);assert.equal(s.objects.size,1);
});
test('storage failure leaves the receipt untouched and the same upload can be retried',async()=>{
 const s=await setup(),p=await s.plan(),j=await s.start([p.id]),id=crypto.randomUUID(),original=s.env.R2_BUCKET.put;
 s.env.R2_BUCKET.put=async()=>{throw Error('Fixture storage unavailable');};assert.equal((await s.upload(j.job_id,p.id,'inbound_plan',{client_req_id:id})).ok,false);
 assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_attachments').get().n,0);assert.equal((await s.ok('v2_ops_job_detail',{job_id:j.job_id})).job.status,'working');
 s.env.R2_BUCKET.put=original;assert.equal((await s.upload(j.job_id,p.id,'inbound_plan',{client_req_id:id})).ok,true);
 s.DB.raw.exec("CREATE TRIGGER fixture_photo_fail BEFORE INSERT ON v2_attachments BEGIN SELECT RAISE(ABORT,'fixture write failure'); END");assert.equal((await s.upload(j.job_id,p.id)).ok,false);assert.equal(s.objects.size,1);
});
