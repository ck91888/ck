// Local browser acceptance against real handlers and built assets. Ephemeral SQLite only.
import assert from 'node:assert/strict';import http from 'node:http';import fs from 'node:fs';import path from 'node:path';import {createRequire} from 'node:module';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_ATTENDANCE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'},root=path.resolve('worker-v2/.sop-staging-assets');
const call=async(action,b={})=>{const r=await worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...b})}),env);const v=await r.json();assert.equal(v.ok,true,action+': '+v.error);return v;};
const jobs=[];for(const [i,action,extra] of [[0,'v2_ops_job_start',{job_type:'pack_direct',flow_stage:'outbound'}],[1,'v2_pick_job_start',{pick_doc_nos:['QA-BROWSER-PICK']}],[2,'v2_unplanned_unload_start',{biz_class:'bulk'}],[3,'v2_bulk_op_job_start',{work_order_no:'QA-BROWSER-EXTERNAL',customer:'虚构浏览器客户'}]]){
 const d=(await call('sop_attendance_checkin',{name:'虚构浏览器人员'+i,agency:'가온'})).record,staff={id:d.badgeId,name:d.name};
 const j=await call('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...extra},workers:[staff],lead_id:staff.id,estimated_minutes:10,labor_department:'bulk'});jobs.push({...j,d,staff});
}
const courierDay=(await call('sop_attendance_checkin',{name:'虚构快递人员',agency:'가온'})).record;
await call('sop_courier_config');const courier=(await call('sop_courier_receive',{operation:'start',workers:[{id:courierDay.badgeId,name:courierDay.name}],lead_id:courierDay.badgeId})).batch;
const server=http.createServer(async(req,res)=>{try{const u=new URL(req.url,'http://127.0.0.1');if(u.pathname.endsWith('/api')){let b='';for await(const c of req)b+=c;const r=await worker.fetch(new Request('http://127.0.0.1'+u.pathname,{method:'POST',headers:{'Content-Type':'application/json'},body:b}),env);res.writeHead(r.status,{'Content-Type':'application/json'});res.end(await r.text());return;}let f=path.resolve(root,'.'+decodeURIComponent(u.pathname));if(!f.startsWith(root+path.sep))throw Error('path denied');if(fs.statSync(f).isDirectory())f=path.join(f,'index.html');res.setHeader('Content-Type',({'.js':'text/javascript','.html':'text/html','.css':'text/css'})[path.extname(f)]||'application/octet-stream');res.end(fs.readFileSync(f));}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),page=await browser.newPage({viewport:{width:390,height:844}}),errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.accept());
await page.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
const active=id=>DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(id).n;
const waitStatus=async(id,status)=>{await page.waitForFunction(async({id,status})=>{const r=await window.CKSession.request('v2_ops_job_detail',{job_id:id});return r.job.status===status;},{id,status});};
fs.mkdirSync('/tmp/ck-pause-browser',{recursive:true});
try{
 await page.goto(origin+'/001/');await page.waitForFunction(()=>window.CKSession?.user&&window.CKOpenNativeJob);
 for(const [i,j] of jobs.entries()){
  // Exercise actual home-card click after one-person rest, with no saved lead.
  await call('sop_attendance_break_start',{id:j.d.id});await page.reload();await page.waitForFunction(()=>window.CKOpenNativeJob&&window.CKSession?.user);
  const detail=await call('v2_ops_job_detail',{job_id:j.job_id});const label=await page.evaluate(d=>window.CKDocumentLabels.jobLabel(d.job),detail);
  const card=page.locator('.ck-native-task').filter({has:page.locator('strong',{hasText:label})}).first();await card.waitFor();await card.click();
  await page.getByRole('button',{name:/暂停整个任务/}).first().waitFor();assert.equal(active(j.job_id),0);
  await page.getByRole('button',{name:/暂停整个任务/}).first().click();await page.locator('dialog[open] textarea[name=reason]').fill('虚构午间暂停');await page.locator('dialog[open] button[type=submit]').click();await waitStatus(j.job_id,'paused');
  await call('sop_attendance_break_end',{id:j.d.id});assert.equal(active(j.job_id),0);
  await page.reload();await page.waitForFunction(()=>window.CKOpenNativeJob&&window.CKSession?.user);await page.locator('.ck-native-task').filter({has:page.locator('strong',{hasText:label})}).first().click();
  await page.getByRole('button',{name:/恢复整个任务/}).first().click();const form=page.locator('dialog[open] form');await form.locator('[data-staff-badge]').fill(j.staff.id+'|'+j.staff.name);await form.locator('[data-staff-add]').click();await form.locator('button[type=submit]').click();await waitStatus(j.job_id,'working');assert.equal(active(j.job_id),1);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  await page.screenshot({path:'/tmp/ck-pause-browser/job-'+i+'-mobile.png',fullPage:true});
  await page.evaluate(()=>goPage('home'));
 }
 await page.evaluate(id=>CKOpenNativeJob({id,job_type:'courier_receiving'}),courier.id);await page.getByRole('button',{name:/取消/}).last().click().catch(()=>{});
 await page.locator('.courier-field [data-pause]').click();await page.locator('dialog[open] textarea[name=reason]').fill('虚构快递暂停');await page.locator('dialog[open] button[type=submit]').click();await waitStatus(courier.id,'paused');assert.equal(active(courier.id),0);
 await page.locator('.courier-field [data-pause]').click();await page.locator('dialog[open] button[type=submit]').click();await waitStatus(courier.id,'working');assert.equal(active(courier.id),1);
 await page.setViewportSize({width:1365,height:900});await page.screenshot({path:'/tmp/ck-pause-browser/courier-desktop.png',fullPage:true});
 assert.deepEqual(errors,[]);console.log('Browser PASS: home-card → resting original job → pause → end personal break without starting → refresh → confirm crew and resume; packing, picking, unloading, bulk; courier pause/resume; mobile and desktop.');
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
