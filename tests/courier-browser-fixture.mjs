// Local, fictional acceptance only. No remote storage, credentials or records.
import https from 'node:https';import fs from 'node:fs';import path from 'node:path';
import entry from '../worker-v2/staging-entry.js';import {database} from './d1-adapter.mjs';import {digest} from '../worker-v2/access-control.js';
const root=path.resolve('worker-v2/.sop-staging-assets'),DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ADMIN_CODE_SHA256:await digest('local-browser-fixture-only')};
const port=Number(process.env.CK_COURIER_FIXTURE_PORT||7833),origin='https://127.0.0.1:'+port;let cookie='';
const call=async(action,b={})=>{const r=await entry.fetch(new Request(origin+'/api',{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookie},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);if(action==='sop_login')cookie=r.headers.get('set-cookie').split(';')[0];const out=await r.json();if(!out.ok)throw Error(out.error);return out;};
await call('sop_login',{sop_key:'local-browser-fixture-only'});const staff=[];
for(let i=0;i<3;i++){const p=(await call('sop_attendance_employee_register',{name:['隔离派工员','隔离扫描员','가상작업자'][i],employeeNo:'BROWSER-'+i,department:'direct_ship'})).person;await call('sop_attendance_checkin',{badge:p.badgeId});staff.push({id:p.badgeId,name:p.name});}
await call('sop_courier_config');DB.raw.exec("INSERT INTO ck_courier_receipts(id,tracking_no,owner,received_at,scanner_id,scanner_name,actor_id,actor_name) VALUES('BROWSER-HISTORY','301000009999','unknown','2020-01-01T01:00:00Z','OLD','历史人员','OLD','历史人员')");
fs.writeFileSync('/tmp/ck-courier-browser-staff.json',JSON.stringify(staff));
if(process.env.CK_COURIER_EXPORT_FIXTURE==='true'){
 const name='=1+1, "隔离&<한>"',p=(await call('sop_attendance_employee_register',{name:'导出隔离人员',employeeNo:'EXPORT-SPECIAL',department:'direct_ship'})).person;await call('sop_attendance_checkin',{badge:p.badgeId});
 const special={id:p.badgeId,name:p.name},batches=[];
 const codes=['0012345678901','JJD123456789012345678',...Array.from({length:203},(_,i)=>'701000000'+(201+i))];
 for(const group of [codes,[],['799000000999']]){
  const {batch}=await call('sop_courier_receive',{operation:'start',workers:[special,staff[1]],lead_id:special.id});
  for(const tracking_no of group)await call('sop_courier_receive',{batch_id:batch.id,scanner_badge:special.id,tracking_no,owner:tracking_no.startsWith('JJD')?'tent':'8-1'});
  batches.push((await call('sop_courier_receive',{operation:'finish',batch_id:batch.id,revision:batch.revision})).batch);
 }
 // Fixed timestamps in fictional storage check both sides of the Korean midnight.
 DB.raw.prepare('UPDATE ck_courier_receipts SET scanner_name=? WHERE id IN (SELECT receipt_id FROM ck_courier_batch_items WHERE batch_id=?)').run(name,batches[0].id);
 DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE tracking_no=?').run('2026-10-07T14:59:59Z',codes[0]);
 DB.raw.prepare('UPDATE ck_courier_receipts SET received_at=? WHERE tracking_no=?').run('2026-10-07T15:00:00Z',codes[1]);
 fs.writeFileSync('/tmp/ck-courier-export-fixture.json',JSON.stringify({batches,scanner:name,codes}));
}
env.ASSETS={async fetch(request){const url=new URL(request.url);let file=path.resolve(root,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(root+path.sep)&&file!==root)return new Response('invalid', {status:400});try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html','.js':'text/javascript','.css':'text/css','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}}};
https.createServer({key:fs.readFileSync('/tmp/ck-local-test.key'),cert:fs.readFileSync('/tmp/ck-local-test.crt')},async(req,res)=>{try{const body=[];for await(const c of req)body.push(c);const method=req.method,request=new Request(origin+req.url,{method,headers:req.headers,...(['GET','HEAD'].includes(method)?{}:{body:Buffer.concat(body)})}),result=await entry.fetch(request,env);const headers=Object.fromEntries(result.headers);const cookies=result.headers.getSetCookie();if(cookies.length)headers['set-cookie']=cookies;res.writeHead(result.status,headers);res.end(Buffer.from(await result.arrayBuffer()));}catch(e){res.statusCode=500;res.end(e.message);}}).listen(port,'127.0.0.1',()=>console.log('Isolated courier acceptance on localhost:'+port));
