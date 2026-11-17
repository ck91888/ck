// Actual shared UI in Chromium with in-memory fictional responses. No Worker, remote API or business writes.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createRequire} from 'node:module';
import {report,DAY} from './workforce-paging-data.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const source=path.resolve(process.env.CK_FE07_SOURCE_ROOT||'.'),out=process.env.CK_FE07_OUT||'/tmp/ck-workforce-paging',paged=process.env.CK_FE07_EXPECT_PAGED==='true';
const css=process.env.CK_FE07_CSS||'worker-v2/.sop-staging-assets/shared/staging-common.css';
fs.mkdirSync(out,{recursive:true});const results=[],browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
try{for(const spec of [{count:100,field:false,width:1440},{count:1000,field:false,width:1440},{count:10000,field:false,width:1440},{count:10000,field:true,width:390}]){
 const context=await browser.newContext({viewport:{width:spec.width,height:900}}),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 try{
  await page.route('**/*',r=>r.request().url()==='https://workforce-fixture.invalid/'?r.fulfill({contentType:'text/html',body:'<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1"></head><body><main><section id="root"></section></main></body></html>'}):r.abort());await page.goto('https://workforce-fixture.invalid/');
  await page.addStyleTag({path:css});
  await page.evaluate(({data,day})=>{const interval=setInterval.bind(window);window.setInterval=(fn,ms,...args)=>ms===15000?-1:interval(fn,ms,...args);window.__report=data;window.__calls=[];window.__downloads=[];window.__long=[];window.__observer=new PerformanceObserver(list=>__long.push(...list.getEntries().map(e=>e.duration)));__observer.observe({type:'longtask',buffered:true});
   CKSession={ready:Promise.resolve(),user:{id:'SYN-MANAGER',role:'manager'},request:async(action,payload)=>{__calls.push({action,payload});if(action==='sop_attendance_config')return {day,asOf:data.asOf,agencies:['가온','포레인'],user:{id:'SYN-MANAGER',role:'manager'}};if(action==='sop_attendance_summary')return structuredClone(__report);throw Error('Unexpected synthetic action '+action);}};
   URL.createObjectURL=blob=>{__downloads.push(blob);return 'blob:synthetic-export';};URL.revokeObjectURL=()=>{};HTMLAnchorElement.prototype.click=function(){};
  },{data:report(spec.count),day:DAY});
  for(const file of ['shared/document-labels.js','shared/attendance-ui.js'])await page.addScriptTag({content:fs.readFileSync(path.join(source,file),'utf8')});
  const mounted=await page.evaluate(async field=>{const start=performance.now();await CKLabor(document.querySelector('#root'),{field});const jsMs=performance.now()-start;await new Promise(requestAnimationFrame);await new Promise(requestAnimationFrame);return {jsMs,paintMs:performance.now()-start,nodes:document.querySelectorAll('#root *').length,rows:document.querySelectorAll('[data-rows] [data-person]').length,cards:document.querySelectorAll('[data-peoplecards] [data-person]').length};},spec.field);
  assert.equal(mounted.rows,paged?Math.min(50,spec.count):spec.count);assert.equal(mounted.cards,spec.field?mounted.rows:0);
  const measured=[];for(const [selector,value] of [['search','Synthetic person 000'],['search',''],['company','가온'],['company','']]){
   const row=await page.evaluate(async({selector,value})=>{const root=document.querySelector('#root'),input=root.querySelector('[data-'+selector+']'),start=performance.now();input.value=value;input.dispatchEvent(new Event(selector==='search'?'input':'change',{bubbles:true}));const jsMs=performance.now()-start;await new Promise(requestAnimationFrame);await new Promise(requestAnimationFrame);return {selector,value,jsMs,paintMs:performance.now()-start,ids:[...root.querySelectorAll('[data-rows] [data-person]')].map(b=>b.dataset.person),summary:root.querySelector('[data-summary]').textContent,agencies:root.querySelector('[data-agencies]').textContent};},{selector,value});
   const expected=report(spec.count).items.filter(x=>selector==='search'?x.record.name.toLowerCase().includes(value.toLowerCase()):!value||x.record.personType!=='employee'&&x.record.agency===value).map(x=>x.record.id);
   assert.deepEqual(row.ids,paged?expected.slice(0,50):expected);assert.match(row.summary,new RegExp('当天签到 / 출근'+expected.length+' 人'));row.displayed=row.ids.length;delete row.ids;measured.push(row);
  }
  const unchanged=await page.evaluate(async()=>{document.querySelector('[data-export]')?.click();return {calls:__calls.map(x=>x.action),csv:__downloads.length?await __downloads.at(-1).text():null,longTasks:__long.length,maxLongTask:Math.max(0,...__long),unchanged:JSON.stringify(__report)};});
  if(spec.field)assert.equal(unchanged.csv,null);else assert.equal(unchanged.csv.split('\r\n').length,spec.count+1);assert.equal(unchanged.unchanged,JSON.stringify(report(spec.count)));assert.deepEqual(unchanged.calls,['sop_attendance_config','sop_attendance_summary']);assert.deepEqual(errors,[]);
  const row={...spec,paged,mounted,filter:measured.map(({summary,agencies,...x})=>x),longTasks:unchanged.longTasks,maxLongTask:unchanged.maxLongTask,exported:spec.field?0:spec.count,requests:unchanged.calls.length};results.push(row);console.log(JSON.stringify(row));
  await page.screenshot({path:path.join(out,'people-'+spec.count+'-'+spec.width+'.png'),fullPage:false});
 }finally{await page.evaluate(()=>document.querySelector('#root').__ckLabor?.destroy()).catch(()=>{});await context.close();}
}fs.writeFileSync(path.join(out,'result.json'),JSON.stringify({synthetic:true,uiOnly:true,paged,source,results},null,2));}finally{await browser.close();}
