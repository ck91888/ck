// Loopback-only acceptance: real staging entry, built pages and synthetic SQLite.
// The policy flag below is local to this process; never deploy this fixture.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import entry from '../worker-v2/staging-entry.js';
import {database} from './d1-adapter.mjs';
import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';

const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),assets=path.resolve('worker-v2/.sop-staging-assets');
assert.ok(fs.existsSync(path.join(assets,'shuju/index.html')),'Build staging assets first.');
const artifacts=path.resolve(process.env.CK_SHIFT_ARTIFACTS||path.join(tmpdir(),'ck-attendance-shift-evidence'));
fs.mkdirSync(artifacts,{recursive:true});
const day=new Date(Date.now()+9*3600000-86400000).toISOString().slice(0,10),at=time=>new Date(day+'T'+time+':00+09:00').toISOString();
const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_ADMIN_CODE_SHA256:await digest('synthetic-shift-fixture-only')};
let cookie='';
async function call(action,body={}){
 const response=await entry.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),env);
 const next=response.headers.getSetCookie().find(value=>value.startsWith('ck_office_access='));if(next)cookie=next.split(';')[0];
 const result=rememberOperation(await response.json(),cookie);assert.equal(result.ok,true,action+': '+(result.message||result.error));return result;
}
await call('sop_login',{sop_key:'synthetic-shift-fixture-only'});await confirmOperation(call,'合成考勤验收员');
const daily=(await call('sop_attendance_checkin',{name:'合成日当班次',agency:'가온'})).record;
const employee=(await call('sop_attendance_employee_register',{employeeNo:'SYNTHETIC-SHIFT-E1',name:'合成职员班次',department:'bulk'})).person;
const employeeDay=(await call('sop_attendance_checkin',{badge:employee.badgeId})).record;
const outsideMorning=(await call('sop_attendance_checkin',{name:'合成窗口外上午',agency:'가온'})).record;
const noonPerson=(await call('sop_attendance_employee_register',{employeeNo:'SYNTHETIC-SHIFT-E2',name:'合成正午窗口外职员',department:'bulk'})).person;
const noonDay=(await call('sop_attendance_checkin',{badge:noonPerson.badgeId})).record;
for(const [r,arrival,breakStart,breakEnd,periods] of [[daily,'08:50','12:00','13:00',[['09:00','12:00'],['13:00','17:00']]],[employeeDay,'09:20','12:00','13:00',[['09:30','12:00'],['13:00','17:00']]],[outsideMorning,'11:50','12:00','12:20',[['13:00','17:00']]],[noonDay,'12:00','16:00','16:15',[['13:00','16:00'],['16:15','17:00']]]]){
 DB.raw.prepare('UPDATE ck_attendance_days SET day=?,signed_in=?,signed_out=? WHERE id=?').run(day,at(arrival),at('18:00'),r.id);
 DB.raw.prepare('INSERT INTO ck_attendance_breaks VALUES(?,?,?,?,?,?)').run('SYNTHETIC-BREAK-'+r.id,r.id,at(breakStart),at(breakEnd),'[]','SYNTHETIC');
 const job='SYNTHETIC-SHIFT-JOB-'+r.id;
 DB.raw.prepare("INSERT INTO v2_ops_jobs(id,job_type,biz_class,status,display_no) VALUES(?,'bulk_op','bulk','completed',?)").run(job,job);
 for(const [i,[start,end]] of periods.entries())DB.raw.prepare('INSERT INTO v2_ops_job_workers(id,job_id,worker_id,worker_name,joined_at,left_at,minutes_worked) VALUES(?,?,?,?,?,?,?)').run(job+'-'+i,job,r.badgeId,r.name,at(start),at(end),(Date.parse(at(end))-Date.parse(at(start)))/60000);
}
const snapshot=()=>Object.fromEntries(['ck_attendance_days','ck_attendance_breaks','v2_ops_job_workers'].map(table=>[table,DB.raw.prepare('SELECT * FROM '+table+' ORDER BY rowid').all()]));
const before=snapshot();
env.ASSETS={async fetch(request){
 const url=new URL(request.url);let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));
 if(!file.startsWith(assets+path.sep))return new Response('invalid',{status:400});
 try{if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');return new Response(fs.readFileSync(file),{headers:{'Content-Type':({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json'})[path.extname(file)]||'application/octet-stream'}});}catch{return new Response('not found',{status:404});}
}};
const server=http.createServer(async(req,res)=>{try{
 const chunks=[];for await(const chunk of req)chunks.push(chunk);
 const response=await entry.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(['GET','HEAD'].includes(req.method)?{}:{body:Buffer.concat(chunks)})}),env);
 const headers=Object.fromEntries(response.headers);if(response.headers.getSetCookie().length)headers['set-cookie']=response.headers.getSetCookie();
 res.writeHead(response.status,headers);res.end(new Uint8Array(await response.arrayBuffer()));
}catch(error){res.writeHead(500);res.end(error.message);}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:process.env.CK_BROWSER_EXECUTABLE||'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const errors=[],scenarios=[];
const pass=name=>{scenarios.push(name);console.log('PASS '+name);};
function parseCsv(csv){const rows=csv.replace(/^\ufeff/,'').split(/\r?\n/).filter(Boolean).map(line=>[...line.matchAll(/"((?:[^"]|"")*)"(?:,|$)/g)].map(x=>x[1].replaceAll('""','"')));return rows.slice(1).map(row=>Object.fromEntries(rows[0].map((key,i)=>[key,row[i]])));}
async function exported(page,button){const pending=page.waitForEvent('download');await button.click();const download=await pending,csv=fs.readFileSync(await download.path(),'utf8');fs.writeFileSync(path.join(artifacts,download.suggestedFilename()),csv);return parseCsv(csv);}
function checkCsv(row,arrival,presenceColumn){assert.equal(row['上班'],at(arrival));assert.equal(row['下班'],at('18:00'));assert.equal(row[presenceColumn],'540');assert.equal(row['休息分钟'],'90');assert.equal(row['有效上岗（韩国时间）'],day+' 09:00 KST');assert.equal(row['班次'],'09:00');assert.equal(row['实际登记休息分钟'],'60');assert.equal(row['签退补足休息分钟'],'30');assert.equal(row['休息尚不足分钟'],'0');}
try{
 for(const width of [1440,390]){
  const context=await browser.newContext({viewport:{width,height:width===390?844:1000}});
  await context.addCookies([{name:cookie.split('=')[0],value:cookie.slice(cookie.indexOf('=')+1),url:origin}]);
  const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',error=>errors.push(error.message));
  await page.route('**/*',route=>new URL(route.request().url()).origin===origin?route.continue():route.abort());
  await page.goto(origin+'/shuju/?tab=labor');const labor=page.locator('.ck-labor');await labor.locator('[data-date]').waitFor();
  await labor.locator('[data-date]').fill(day);await labor.locator('[data-date]').dispatchEvent('change');
  const dailyRow=labor.locator('[data-rows] tr').filter({hasText:daily.name});await dailyRow.waitFor();
  assert.match(await dailyRow.innerText(),/08:50 – 18:00/);assert.match(await dailyRow.locator('[data-effective-attendance]').innerText(),/09:00/);
  assert.match(await labor.locator('[data-time-rule]').innerText(),/签退后从未归属补足/);
  const dailyRows=await exported(page,labor.locator('[data-export]')),dailyCsv=dailyRows.find(row=>row['姓名']===daily.name);checkCsv(dailyCsv,'08:50','在岗分钟');assert.equal(dailyCsv['大货分钟'],'420');
  const outsideCsv=dailyRows.find(row=>row['姓名']===outsideMorning.name);assert.equal(outsideCsv['上班'],at('11:50'));assert.equal(outsideCsv['有效上岗（韩国时间）'],day+' 11:50 KST');assert.equal(outsideCsv['在岗分钟'],'370');assert.equal(outsideCsv['大货分钟'],'240');assert.equal(outsideCsv['默认休息目标分钟'],'90');assert.equal(outsideCsv['实际登记休息分钟'],'20');assert.equal(outsideCsv['签退补足休息分钟'],'70');assert.equal(outsideCsv['休息分钟'],'90');
  await dailyRow.locator('[data-person]').click();const dailyDetail=labor.locator('[data-detail-dialog]');await dailyDetail.waitFor();
  assert.match(await dailyDetail.locator('[data-attendance-accounting]').innerText(),/实际登记休息.*1:00/);assert.match(await dailyDetail.locator('[data-attendance-accounting]').innerText(),/签退补足.*0:30/);
  assert.equal(await dailyDetail.locator('.ck-timeline li').filter({hasText:'休息'}).count(),1,'automatic rest must not invent a physical break interval');
  await page.screenshot({path:path.join(artifacts,'daily-detail-'+width+'.png'),fullPage:true});await dailyDetail.locator('[data-close-detail]').click();
  pass('daily '+width+': raw/effective punches, 420 work minutes, real 60 + automatic 30, unchanged real break timeline and CSV');
  await labor.locator('[data-rows] tr').filter({hasText:outsideMorning.name}).locator('[data-person]').click();await dailyDetail.waitFor();assert.match(await dailyDetail.locator('[data-attendance-accounting]').innerText(),/签退补足.*1:10/);await dailyDetail.locator('[data-close-detail]').click();pass('historical outside-window morning '+width+': actual 11:50 start and 90-minute target agree in list/detail/CSV');
  await page.goto(origin+'/shuju/?tab=employees');const staff=page.locator('.ck-employee-dashboard');await staff.locator('[data-date]').waitFor();
  await staff.locator('[data-date]').fill(day);await staff.locator('[data-date]').dispatchEvent('change');
  const staffRow=staff.locator('[data-table] tbody tr').filter({hasText:employee.employeeNo});await staffRow.waitFor();
  assert.match(await staffRow.locator('.ck-employee-punch').innerText(),/09:20.*18:00/s);assert.match(await staffRow.locator('[data-effective-attendance]').innerText(),/09:00/);
  const employeeRows=await exported(page,staff.locator('[data-export]')),employeeCsv=employeeRows.find(row=>row['工号']===employee.employeeNo);checkCsv(employeeCsv,'09:20','出勤分钟');
  const noonCsv=employeeRows.find(row=>row['工号']===noonPerson.employeeNo);assert.equal(noonCsv['上班'],at('12:00'));assert.equal(noonCsv['有效上岗（韩国时间）'],day+' 12:00 KST');assert.equal(noonCsv['出勤分钟'],'360');assert.equal(noonCsv['默认休息目标分钟'],'40');assert.equal(noonCsv['实际登记休息分钟'],'15');assert.equal(noonCsv['签退补足休息分钟'],'25');assert.equal(noonCsv['休息分钟'],'40');
  await staffRow.locator('[data-work-detail-open]').click();const employeeDetail=staff.locator('[data-work-detail-dialog]');await employeeDetail.locator('[data-workday-timeline]').waitFor();
  assert.match(await employeeDetail.locator('[data-attendance-accounting]').innerText(),/实际登记休息.*1:00/);assert.match(await employeeDetail.locator('[data-attendance-accounting]').innerText(),/签退补足.*0:30/);
  assert.equal(await employeeDetail.locator('[data-workday-timeline] li').filter({hasText:'已登记休息'}).count(),1);
  assert.match(await employeeDetail.locator('.ck-workday-metrics').innerText(),/6:30/);
  const fits=await employeeDetail.evaluate(node=>{const r=node.getBoundingClientRect();return r.left>=0&&r.top>=0&&r.right<=innerWidth+1&&r.bottom<=innerHeight+1&&node.scrollWidth<=node.clientWidth+1;});assert.equal(fits,true,'detail must fit viewport');
  await page.screenshot({path:path.join(artifacts,'employee-detail-'+width+'.png'),fullPage:true});await employeeDetail.locator('[data-close-work-detail]').click();
  pass('employee '+width+': raw/effective punches, 390 real work minutes, matching accounting/detail/CSV and fitting modal');
  await staff.locator('[data-table] tbody tr').filter({hasText:noonPerson.employeeNo}).locator('[data-work-detail-open]').click();await employeeDetail.locator('[data-workday-timeline]').waitFor();assert.match(await employeeDetail.locator('[data-attendance-accounting]').innerText(),/实际登记休息.*0:15/);assert.match(await employeeDetail.locator('[data-attendance-accounting]').innerText(),/签退补足.*0:25/);assert.match(await employeeDetail.locator('[data-attendance-accounting]').innerText(),/目标.*0:40/);assert.match(await employeeDetail.locator('.ck-workday-metrics').innerText(),/3:45/);await page.screenshot({path:path.join(artifacts,'employee-noon-detail-'+width+'.png'),fullPage:true});await employeeDetail.locator('[data-close-work-detail]').click();pass('historical outside-window noon '+width+': actual 12:00 start and 40-minute target agree in list/detail/CSV; 225 work minutes retained');
  await context.close();
 }
 assert.deepEqual(snapshot(),before,'page reads and exports must not change punches, breaks or job clocks');assert.deepEqual(errors,[]);
 pass('no business row changes or browser exceptions');fs.writeFileSync(path.join(artifacts,'result.json'),JSON.stringify({ok:true,day,scenarios},null,2));
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));DB.raw.close();}
