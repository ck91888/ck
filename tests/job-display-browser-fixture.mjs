import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
// Local, fictional acceptance only. No remote storage, credentials or records.
import http from 'node:http';import fs from 'node:fs';import path from 'node:path';
import entry from '../worker-v2/staging-entry.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
const root=path.resolve('worker-v2/.sop-staging-assets'),DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('local-browser-fixture-only')};
const port=Number(process.env.CK_JOB_DISPLAY_FIXTURE_PORT||7836),origin='http://127.0.0.1:'+port;let cookie='';
const call=async(action,b={})=>{const r=await entry.fetch(new Request(origin+'/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];const out=rememberOperation(await r.json(),cookie);if(!out.ok)throw Error(out.error);return out;};
await call('sop_login',{sop_key:'local-browser-fixture-only'});await confirmOperation(call,'演示派审员金');const staff=[];
for(let i=0;i<3;i++){const p=(await call('sop_attendance_employee_register',{name:['演示派审员金','演示员工甲','演示员工乙'][i],employeeNo:'BROWSER-'+i,department:'direct_ship'})).person;await call('sop_attendance_checkin',{badge:p.badgeId});staff.push({id:p.badgeId,name:p.name});}
const config=await call('sop_attendance_config'),today=config.day;
const dayStart=Date.parse(today+'T00:00:00+09:00'),arrival=new Date(Math.max(dayStart,Date.now()-3*3600000)).toISOString(),workStart=new Date(Math.max(dayStart,Date.now()-2*3600000)).toISOString();
const daily=[];
for(const name of ['演示待安排甲','演示作业中乙','가상 휴식 丙','演示已下班丁','演示误签到戊','演示已删除己']){
 let r=(await call('sop_attendance_checkin',{name,agency:'가온'})).record;
 r=(await call('sop_attendance_correct',{id:r.id,version:r.version,inAt:arrival,outAt:'',reason:'隔离验收虚拟时间'})).record;daily.push(r);
}
await call('sop_attendance_department',{id:daily[0].id,version:daily[0].version,department:'bulk'});
await call('sop_attendance_department',{id:daily[1].id,version:daily[1].version,department:'import'});
const fieldLogin=await entry.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin},body:JSON.stringify({action:'sop_login',badge:staff[0].id})}),env);
const fieldCookie=fieldLogin.headers.get('set-cookie')?.split(';')[0];if(!fieldCookie)throw Error('Isolated field fixture login failed');
const fieldCall=async(action,b={})=>{const r=await entry.fetch(new Request(origin+'/001/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:fieldCookie},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env),v=await r.json();if(!v.ok)throw Error(v.error);return v;};
const activeCrew=[{id:daily[1].badgeId,name:daily[1].name},staff[0]];
const started=await fieldCall('sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'DEMO-WMS-7301',customer:'隔离演示客户',client_req_id:crypto.randomUUID()},workers:activeCrew,lead_id:activeCrew[0].id,estimated_minutes:30,labor_department:'bulk'});
// Historical source-less clock: keep its real operation type, date and missing dispatcher.
const oldId='JOB-'+crypto.randomUUID(),oldEnd=new Date(Math.max(dayStart,Date.now()-3600000)).toISOString();
DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,created_at,finished_at) VALUES(?,'inventory','bulk','completed',?,?)").run(oldId,workStart,oldEnd);
DB.raw.prepare("INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked,leave_reason) VALUES(?,?,?,?,?,?,60,'job_completed')").run(crypto.randomUUID(),oldId,daily[3].badgeId,daily[3].name,workStart,oldEnd);
await confirmOperation(call,'演示办公室李');
await call('sop_attendance_break_start',{id:daily[2].id});await call('sop_attendance_checkout',{id:daily[3].id});
await call('sop_attendance_void',{id:daily[5].id,version:daily[5].version,reason:'隔离演示误签到，可恢复'});
await call('sop_attendance_break_start',{badge:staff[1].id});await call('sop_attendance_checkout',{badge:staff[2].id});
env.ASSETS={async fetch(request){const url=new URL(request.url);let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep)&&file!==root)return new Response('invalid', {status:400});try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}}};
http.createServer(async(req,res)=>{try{const body=[];for await(const c of req)body.push(c);// This loopback-only fixture deliberately supplies its synthetic office session. Never deploy it.
const method=req.method,request=new Request(origin+req.url,{method,headers:{...req.headers,cookie:req.url.startsWith('/001/')?fieldCookie:cookie},...(['GET','HEAD'].includes(method)?{}:{body:Buffer.concat(body)})}),result=await entry.fetch(request,env);const headers=Object.fromEntries(result.headers);const cookies=result.headers.getSetCookie();if(cookies.length)headers['set-cookie']=cookies;headers['set-cookie']=(req.url.startsWith('/001/')?fieldCookie:cookie)+'; Path=/; SameSite=Lax';res.writeHead(result.status,headers);res.end(Buffer.from(await result.arrayBuffer()));}catch(e){res.statusCode=500;res.end(e.message);}}).listen(port,'127.0.0.1',()=>console.log('Isolated job display acceptance on localhost:'+port));

