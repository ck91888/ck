// Shipped staging assets + real Worker, synthetic employees and localhost only.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {officeDutyFixture} from './office-duty-fixture.mjs';
const {chromium}=createRequire(import.meta.url)('playwright');
const f=await officeDutyFixture(),assets=path.resolve('worker-v2/.sop-staging-assets');
const artifacts=process.env.CK_OFFICE_ARTIFACTS||'/tmp/ck-office-duty-browser';fs.mkdirSync(artifacts,{recursive:true});
const server=http.createServer(async(req,res)=>{try{
 if(req.url.split('?')[0].endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await f.worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),f.env);res.writeHead(r.status,Object.fromEntries(r.headers));res.end(new Uint8Array(await r.arrayBuffer()));return;}
 let file=path.resolve(assets,'.'+decodeURIComponent(req.url.split('?')[0]));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');res.end(fs.readFileSync(file));
 }catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),errors=[],evidence=[];
try{
 for(const width of [1440,390,320]){
  const context=await browser.newContext({viewport:{width,height:980},acceptDownloads:true}),[name,value]=f.cookies.office.split('=');await context.addCookies([{name,value,url:origin}]);const p=await context.newPage();p.setDefaultTimeout(12000);p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>d.accept());
  let losePause=true;
  await p.route('**/*',async route=>{const r=route.request();if(new URL(r.url()).origin!==origin)return route.abort();const body=r.postDataJSON?.();if(body?.action==='sop_office_duty_pause'&&losePause){losePause=false;const response=await route.fetch();assert.equal((await response.json()).ok,true);return route.abort('failed');}return route.continue();});
  await p.goto(origin+'/002/');await p.locator('[data-tab="office_duty"]').click();const root=p.locator('#view-office_duty');
  await root.locator('[data-duty-term]').fill(f.p.employeeNo);await root.locator('[data-duty-search] button').click();await root.locator('[data-duty-people] button').click();
  await root.getByRole('button',{name:'开始值班 / 근무 시작',exact:true}).click();await root.getByRole('button',{name:'暂停 / 중지',exact:true}).waitFor();
  await p.screenshot({path:path.join(artifacts,'working-'+width+'.png'),fullPage:true});
  await root.getByRole('button',{name:'暂停 / 중지',exact:true}).click();await root.getByRole('button',{name:'重试未确认操作 / 미확인 작업 재시도',exact:true}).waitFor();
  assert.equal((await f.get()).job.status,'paused');const segmentsBefore=f.clocks().length;
  await p.reload();await p.locator('[data-tab="office_duty"]').click();await root.getByRole('button',{name:'重试未确认操作 / 미확인 작업 재시도',exact:true}).click();await root.getByRole('button',{name:'继续 / 재개',exact:true}).waitFor();assert.equal(f.clocks().length,segmentsBefore);
  await root.getByRole('button',{name:'继续 / 재개',exact:true}).click();await root.getByRole('button',{name:'暂停 / 중지',exact:true}).waitFor();
  await p.reload();await p.locator('[data-tab="office_duty"]').click();await root.getByRole('button',{name:'结束值班 / 근무 종료',exact:true}).click();await root.getByRole('button',{name:'开始值班 / 근무 시작',exact:true}).waitFor();assert.equal((await f.get()).job.status,'completed');
  const dimensions=await p.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth,entries:document.querySelectorAll('[data-tab="office_duty"]').length}));assert.equal(dimensions.entries,1);assert.ok(dimensions.scroll<=width+1,JSON.stringify(dimensions));
  await p.screenshot({path:path.join(artifacts,'finished-'+width+'.png'),fullPage:true});
  evidence.push({width,lostResponseRecovered:true,reloadRecovered:true,duplicateSegments:false,dimensions});await context.close();
 }
 const context=await browser.newContext({viewport:{width:1440,height:980},acceptDownloads:true}),[name,value]=f.cookies.office.split('=');await context.addCookies([{name,value,url:origin}]);const p=await context.newPage();await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());await p.goto(origin+'/shuju/');await p.evaluate(()=>CKSession.ready);
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);
 const download=p.waitForEvent('download');await p.evaluate(day=>exportActualReport({start_date:day,end_date:day,department:'order_processing'},'tasks'),day);const report=await download,reportFile=path.join(artifacts,'actual-work.csv');await report.saveAs(reportFile);const csv=fs.readFileSync(reportFile,'utf8');assert.match(csv,/time_only/);assert.match(csv,/订单处理/);assert.match(csv,/仅记录工时/);assert.doesNotMatch(csv,/数量未记录（不等于零产出）/);
 await p.evaluate(async()=>{const el=document.createElement('section');el.id='synthetic-employee-export';document.body.append(el);await CKEmployee.dashboard(el);});const employeeDownload=p.waitForEvent('download');await p.locator('#synthetic-employee-export [data-export]').click();const employee=await employeeDownload,employeeFile=path.join(artifacts,'employee-day.csv');await employee.saveAs(employeeFile);const employeeCSV=fs.readFileSync(employeeFile,'utf8');assert.match(employeeCSV,/订单处理分钟（仅工时）/);assert.match(employeeCSV,/SYNTH-DUTY/);evidence.push({actualDateDepartmentCSV:true,employeeDayCSV:true});await context.close();
 assert.deepEqual(errors,[]);fs.writeFileSync(path.join(artifacts,'results.json'),JSON.stringify({evidence,errors},null,2));console.log(JSON.stringify({evidence,errors,artifacts},null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));f.DB.raw.close();}
