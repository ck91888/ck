import test from 'node:test';
import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';
import {handleDailyScan,ensureDailyScan,scanDate,scanToday,scanTotal} from '../worker-v2/import-daily-scan.js';
import worker from '../worker-v2/index.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
const user={id:'synthetic-import',name:'合成进口记录员',role:'dispatcher',scope:'field',departments:['import']};
const date='2026-10-20';
async function setup(t){
 const DB=database();t.after(()=>DB.raw.close());
 const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_REQUEST_USER:user};await ensureDailyScan(env);
 const save=(data={})=>handleDailyScan({action:'sop_import_daily_scan_save',business_date:date,total:12,version:0,client_req_id:crypto.randomUUID(),...data},env);
 const get=(data={})=>handleDailyScan({action:'sop_import_daily_scan_get',business_date:date,...data},env);
 const snapshot=()=>Object.fromEntries(['v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results','v2_inbound_plans','sop_records','sop_events'].map(k=>[k,DB.raw.prepare('SELECT * FROM '+k).all()]));
 return {DB,env,save,get,snapshot};
}
test('date validation and Korean day boundaries are independent of host timezone',()=>{
 assert.equal(scanToday('2026-10-19T14:59:59.999Z'),'2026-10-19');assert.equal(scanToday('2026-10-19T15:00:00Z'),date);
 assert.equal(scanToday('2026-12-31T15:00:00Z'),'2027-01-01');assert.equal(scanDate('2024-02-29'),'2024-02-29');
 for(const value of ['',null,0,'2026-2-01','2026-02-29','2026-02-30','2026-13-01','2026-00-01','2026-01-00','0000-01-01','2026-10-20T00:00:00Z'])assert.throws(()=>scanDate(value),undefined,String(value));
});
test('zero accepted; blank, coerced strings, negatives, fractions and unsafe numbers rejected',()=>{
 assert.equal(scanTotal(0),0);assert.equal(scanTotal(Number.MAX_SAFE_INTEGER),Number.MAX_SAFE_INTEGER);
 for(const value of [undefined,null,'',' ','0','12',true,false,-1,0.5,NaN,Infinity,Number.MAX_SAFE_INTEGER+1,{},[]])assert.throws(()=>scanTotal(value));
});
test('missing versus zero, correction identity/time and no job, inbound or labor side effects',async t=>{
 const f=await setup(t),before=f.snapshot();assert.equal((await f.get()).item,null);
 const a=await f.save({total:0});assert.equal(a.item.total,0);assert.equal(a.item.version,1);
 f.env.SOP_REQUEST_USER={...user,id:'synthetic-other',name:'合成更正员'};
 const b=await f.save({total:7,version:1,actor_id:'forged',actor_name:'forged'}),r=await f.get();
 assert.equal(b.item.total,7);assert.equal(b.item.created_by,user.id);assert.equal(b.item.updated_by,'synthetic-other');assert.ok(Date.parse(b.item.updated_at));
 assert.equal(r.events.length,2);assert.equal(r.events[0].before_total,0);assert.equal(r.events[0].after_total,7);assert.equal(r.events[0].actor_name,'合成更正员');assert.equal(r.events[1].before_total,null);
 assert.equal(r.efficiency,null);assert.deepEqual(f.snapshot(),before);
});
test('request replay never adds total or audit; late replay returns later corrected value',async t=>{
 const f=await setup(t),id=crypto.randomUUID(),a=await f.save({client_req_id:id});await f.save({version:1,total:21});
 const b=await f.save({client_req_id:id});assert.equal(a.saved_version,1);assert.equal(b.saved_version,1);assert.equal(b.item.total,21);assert.equal(b.replayed,true);
 await assert.rejects(f.save({client_req_id:id,total:99}),e=>e.status===409);
 assert.equal((await f.get()).events.length,2);assert.equal((await f.get()).item.total,21);
});
for(const create of [true,false])test(`concurrent ${create?'creation':'correction'} accepts exactly one version; stale input cannot overwrite`,async t=>{
 const f=await setup(t);if(!create)await f.save();
 const r=await Promise.allSettled([f.save({total:23,version:create?0:1}),f.save({total:32,version:create?0:1})]);
 assert.equal(r.filter(x=>x.status==='fulfilled').length,1);assert.equal(r.find(x=>x.status==='rejected').reason.status,409);
 const current=await f.get();assert.equal(current.item.total,23);assert.equal(current.item.version,create?1:2);assert.equal(current.events.length,create?1:2);
});
test('concurrent identical retries only record once',async t=>{
 const f=await setup(t),body={client_req_id:crypto.randomUUID()},r=await Promise.all([f.save(body),f.save(body)]);assert.ok(r.every(x=>x.item.total===12));assert.equal((await f.get()).events.length,1);
});
for(const table of ['ck_import_daily_scan_events','ck_import_daily_scans'])test('failure in '+table+' rolls back audit and daily value and retries safely',async t=>{
 const f=await setup(t),body={client_req_id:crypto.randomUUID()};
 f.DB.raw.exec(`CREATE TRIGGER qa_fail BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT,'synthetic_failure'); END`);
 await assert.rejects(f.save(body));assert.equal((await f.get()).item,null);assert.equal((await f.get()).events.length,0);
 f.DB.raw.exec('DROP TRIGGER qa_fail');assert.equal((await f.save(body)).item.total,12);
});
test('query arbitrary date and bounded audit pages; no historical backfill',async t=>{
 const f=await setup(t);for(let version=0;version<23;version++)await f.save({version,total:version});
 const a=await f.get();assert.equal(a.events.length,20);assert.equal(a.next_before_version,4);
 const b=await f.get({before_version:a.next_before_version});assert.deepEqual(b.events.map(x=>x.version),[3,2,1]);assert.equal(b.next_before_version,null);
 await f.save({business_date:'2024-02-29',total:0});assert.equal((await f.get({business_date:'2024-02-29'})).item.total,0);
 assert.equal((await f.get({business_date:'2024-03-01'})).item,null);assert.equal((await f.get()).recent.length,2);
});
test('invalid API inputs never write',async t=>{
 const f=await setup(t);
 for(const data of [{business_date:undefined},{business_date:''},{business_date:'2026-02-30'},{total:null},{total:''},{total:-2},{total:2.1},{total:'0'},{version:null},{version:-1},{version:'0'},{client_req_id:''}])await assert.rejects(f.save(data));
 assert.equal((await f.get()).item,null);assert.equal((await f.get()).events.length,0);
});
test('roles, department and current daily dispatch grant gate both reads and writes',async t=>{
 const f=await setup(t);
 for(const u of [null,{...user,scope:'kiosk'},{...user,role:'viewer'},{...user,role:'reviewer'},{...user,departments:['bulk']},{...user,daily_dispatch:true,can_dispatch:false,dispatch_departments:['import']},{...user,daily_dispatch:true,can_dispatch:true,dispatch_departments:['bulk']}]){f.env.SOP_REQUEST_USER=u;await assert.rejects(f.save(),e=>e.status===403);await assert.rejects(f.get(),e=>e.status===403);}
 f.env.SOP_REQUEST_USER={...user,daily_dispatch:true,can_dispatch:true,dispatch_departments:['import']};assert.equal((await f.save()).item.total,12);
 f.env.SOP_ENVIRONMENT='production';await assert.rejects(f.get(),e=>e.status===404);
});
test('real Worker cookie auth, field allowlist, office operator, cross origin, session revocation and input forgery',async t=>{
 const DB=database();t.after(()=>DB.raw.close());const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ADMIN_PASSWORD:crypto.randomUUID()};let cookie='';
 const call=async(action,data={},options={})=>{
  const response=await worker.fetch(new Request('https://synthetic.invalid'+(options.path||'/api'),{method:options.method||'POST',headers:{'Content-Type':'application/json',Cookie:options.cookie??cookie,...operationHeaders(cookie),...options.headers},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
  if(action==='sop_login'&&!options.path)cookie=response.headers.get('set-cookie')?.split(';')[0]||'';
  const r=rememberOperation(await response.json(),cookie);return {...r,http:response.status,cookie:response.headers.get('set-cookie')?.split(';')[0]};
 };
 await call('sop_login',{sop_key:env.SOP_ADMIN_PASSWORD});await confirmOperation(call,'合成办公室操作员');
 const p=(await call('sop_attendance_employee_register',{employeeNo:'SYN-SCAN',name:'合成扫描员',department:'import'})).person;
 const day=(await call('sop_attendance_checkin',{badge:p.badgeId})).record;
 const field=(await call('sop_login',{badge:p.badgeId},{path:'/001/api',cookie:''})).cookie;
 const data={business_date:date,total:0,version:0};
 assert.equal((await call('sop_import_daily_scan_save',data,{path:'/001/api',cookie:''})).http,401);
 assert.equal((await call('sop_import_daily_scan_save',{...data,SOP_REQUEST_USER:user},{path:'/001/api',cookie:''})).http,401);
 assert.equal((await call('sop_import_daily_scan_save',data,{path:'/001/api',cookie:field,headers:{Origin:'https://other.invalid'}})).http,403);
 assert.equal((await call('sop_import_daily_scan_save',data,{headers:{'X-CK-Operation-Context':'stale'}})).http,409);
 const saved=await call('sop_import_daily_scan_save',{...data,actor_name:'forged'},{path:'/001/api',cookie:field});assert.equal(saved.ok,true,saved.error);assert.equal(saved.item.updated_name,'合成扫描员');
 assert.equal((await call('sop_import_daily_scan_get',{business_date:date},{path:'/001/api',cookie:field})).item.total,0);
 await call('sop_attendance_checkout',{id:day.id});assert.equal((await call('sop_import_daily_scan_get',{business_date:date},{path:'/001/api',cookie:field})).http,401);
 assert.equal((await call('sop_import_daily_scan_get',{business_date:'2026-02-30'})).http,400);
});
