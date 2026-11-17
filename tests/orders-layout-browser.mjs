// Real Worker + synthetic SQLite + shipped staging assets. All browser requests stay on localhost.
import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import worker from '../worker-v2/index.js';
import {reportFixture} from './actual-work-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require('playwright');
const f=await reportFixture(),{DB,env}=f,artifacts='/tmp/ck-orders-layout';fs.mkdirSync(artifacts,{recursive:true});
const longDoc='SYNTH-LONG-20261012-ABCDEFGHIJKLMNOPQRSTUVWXYZ-'.repeat(5)+'中文한글',longNote='合成备注：长中文与韩文 설명 데이터 결과 확인；<img src=x onerror=alert(1)> '.repeat(40);
const fields=['box_count','pallet_count','packed_sku_count','packed_box_count','total_operated_box_count','label_count','repaired_box_count','reboxed_count','used_carton_large_count','used_carton_small_count','verify_ok_count','verify_ng_count','location_count'];
for(let i=0;i<205;i++){
 const id='SYNTH-'+String(i).padStart(4,'0');f.job(id,i===3?'qc':'pack_direct');f.segment('SEG-'+i,id,'PERSON-'+i,'2026-10-12T00:00:00Z','2026-10-12T00:01:00Z',1);
 if(i!==2)f.result('RESULT-'+i,id,i===0?Object.fromEntries(fields.map((k,n)=>[k,n%3===0?n+10:0])):i===1?Object.fromEntries(fields.map(k=>[k,0])):i===4?{group_outputs:[{process_type:'合成超长中文한글标签'.repeat(30),quantity:17,unit:'箱'}]}:{packed_box_count:5});
 DB.raw.prepare("INSERT INTO sop_records(id,kind,revision,department,state,updated_at) VALUES(?,'dispatch',1,'direct_ship',?,'2026-10-13T00:00:00Z')").run(id,JSON.stringify({labor_department:'direct_ship',owner:i===0?'合成派审员김가나다라마바사'.repeat(4):'合成派审员',title:i===4?'合成任务说明与韩文데이터'.repeat(20):''}));
}
DB.raw.prepare('UPDATE v2_ops_jobs SET display_no=? WHERE id=?').run(longDoc,'SYNTH-0000');DB.raw.prepare('UPDATE v2_ops_job_results SET remark=? WHERE job_id=?').run(longNote,'SYNTH-0000');
const before=JSON.stringify(DB.raw.prepare('SELECT * FROM v2_ops_job_results ORDER BY id').all());
env.SOP_ACCESS_CONTROL='true';env.SOP_ADMIN_PASSWORD=crypto.randomUUID();
// Baseline client is served only to a local comparison page; use an explicit override after future rebases.
const baselineRef=process.env.CK_ORDERS_BASELINE||'9d60a1ccf5810908b4f67e11b972ead30a0dcb31';
const baselineApp=execFileSync('git',['show',baselineRef+':shuju/app.js'],{encoding:'utf8'});
const assets=path.resolve('worker-v2/.sop-staging-assets');
const server=http.createServer(async(req,res)=>{try{
 const url=new URL(req.url,'http://127.0.0.1');
 if(url.pathname.endsWith('/api')){const chunks=[];for await(const c of req)chunks.push(c);const r=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,...(req.method==='POST'?{body:Buffer.concat(chunks)}:{})}),env);res.writeHead(r.status,Object.fromEntries(r.headers));res.end(new Uint8Array(await r.arrayBuffer()));return;}
 let file=path.resolve(assets,'.'+decodeURIComponent(url.pathname));if(!file.startsWith(assets+path.sep))throw Error('path denied');if(fs.statSync(file).isDirectory())file=path.join(file,'index.html');
 res.setHeader('Content-Type',({'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8'})[path.extname(file)]||'application/octet-stream');
 res.end(url.pathname==='/shuju/app.js'&&String(req.headers.referer).includes('layout-baseline=1')?baselineApp:fs.readFileSync(file));
}catch(e){res.writeHead(500);res.end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port,errors=[],evidence=[];
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
const widths=(process.env.CK_ORDERS_WIDTHS||'1600,1440,1024,390,320').split(',').map(Number);
let baseline;
async function openPage(width,isBaseline=false){
 const context=await browser.newContext({viewport:{width,height:1050},acceptDownloads:true});const p=await context.newPage();p.setDefaultTimeout(15000);
 p.on('pageerror',e=>errors.push(e.message));p.on('dialog',d=>{errors.push(d.message());return d.accept();});await p.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
 await p.goto(origin+'/shuju/'+(isBaseline?'?layout-baseline=1':''));await p.locator('#ck-login-code').fill(env.SOP_ADMIN_PASSWORD);await p.locator('#ck-auth-gate button[type=submit]').click();await p.locator('#ck-operator-name').fill('合成验收人员');await p.locator('[data-operator-confirm]').click();await p.locator('#tab-realtime').waitFor({state:'visible'});
 // Pin the same report cutoff for byte-for-byte CSV comparison across both clients.
 await p.evaluate(()=>{const read=api;api=params=>read(Object.assign({},params,params.action.startsWith('v2_dashboard_')?{as_of:'2026-10-19T00:00:00Z'}:{}));});
 return {context,p};
}
async function query(p,basis='actual_work'){
 await p.locator('.tab-bar button').filter({hasText:'单子数据'}).click();await p.locator('#ordersDateBasis').selectOption(basis);await p.locator('#ordersFilterStart').fill('2026-10-12');await p.locator('#ordersFilterEnd').fill('2026-10-18');await p.locator('#tab-orders button').filter({hasText:/^查询$/}).click();await p.waitForFunction(()=>_ordersResult?.total===205&&_ordersItems.length===200);
}
async function download(p,selector,tag){const wait=p.waitForEvent('download');await selector.click();const d=await wait,file=path.join(artifacts,tag+'-'+d.suggestedFilename());await d.saveAs(file);return fs.readFileSync(file,'utf8');}
async function exports(p,tag){
 const out={};out.tasks=await download(p,p.locator('#tab-orders button').filter({hasText:'导出明细 CSV'}),tag);out.quantities=await download(p,p.locator('#tab-orders button').filter({hasText:'导出同类数量样本'}),tag);
 assert.ok(out.tasks.includes(longNote.replaceAll('"','""')));assert.ok(out.tasks.includes('完整结果JSON'));assert.ok(out.tasks.includes('"packed_box_count""')||out.tasks.includes('packed_box_count'));
 await query(p,'created_at');out.history=await download(p,p.locator('#tab-orders button').filter({hasText:'导出明细 CSV'}),tag);
 await p.locator('.tab-bar button').filter({hasText:'工时分析'}).click();await p.locator('#whFilterStart').fill('2026-10-12');await p.locator('#whFilterEnd').fill('2026-10-18');await p.locator('#tab-workhours button').filter({hasText:/^查询$/}).click();await p.waitForFunction(()=>_whSummary?.total===205);
 out.segments=await download(p,p.locator('#tab-workhours button').filter({hasText:'导出明细 CSV'}),tag);return out;
}
try{
 const old=await openPage(1600,true);try{await query(old.p);baseline=await exports(old.p,'baseline');}finally{await old.context.close();}
 for(const width of widths){const {p,context}=await openPage(width);try{
 await query(p);assert.equal(await p.evaluate(()=>_ordersResult.summary.total_seconds),12300);
 const row=p.locator('#ordersBody tr').first(),result=row.locator('.orders-result'),wrap=p.locator('.orders-table-wrap');
 const measure=()=>p.evaluate(()=>{const rows=[...document.querySelectorAll('#ordersBody tr')],r=rows[0],w=document.querySelector('.orders-table-wrap'),cell=r.querySelector('.orders-results');return {width:innerWidth,pageWidth:document.documentElement.scrollWidth,rowHeight:r.getBoundingClientRect().height,maxRowHeight:Math.max(...rows.map(r=>r.getBoundingClientRect().height)),resultWidth:cell.getBoundingClientRect().width,wrapWidth:w.clientWidth,tableWidth:w.scrollWidth,rows:rows.length};});
 const collapsed=await measure();assert.equal(collapsed.rows,200);assert.ok(collapsed.pageWidth<=width+1,JSON.stringify(collapsed));if(width>600){assert.ok(collapsed.maxRowHeight<=150,JSON.stringify(collapsed));assert.ok(collapsed.resultWidth>=298,JSON.stringify(collapsed));}
 await row.scrollIntoViewIfNeeded();await p.screenshot({path:path.join(artifacts,'orders-'+width+'.png')});
 assert.ok((await result.locator('summary').innerText()).includes('含'));await result.locator('summary').click();await p.waitForFunction(()=>document.querySelector('.orders-result').open);assert.equal(await result.locator('.orders-result-full .orders-metric').count(),13);assert.ok((await result.locator('.orders-result-full').innerText()).includes(longNote));assert.equal(await row.locator('img').count(),0);assert.ok(await result.locator('.orders-collapse').isVisible());
 await p.screenshot({path:path.join(artifacts,'expanded-'+width+'.png')});await result.locator('summary').click();assert.equal(await result.getAttribute('open'),null);assert.equal((await measure()).rowHeight,collapsed.rowHeight);
 // Keyboard activation and long business-reference disclosure stay accessible.
 await result.locator('summary').focus();await p.keyboard.press('Enter');assert.equal(await result.getAttribute('open'),'');await p.keyboard.press('Enter');
 const reference=row.locator('.orders-reference');await reference.locator('summary').click();assert.ok((await reference.locator(':scope > div').innerText()).includes(longDoc));await reference.locator('summary').click();
 const zero=p.locator('#ordersBody tr').nth(1);assert.match(await zero.locator('.orders-result summary').innerText(),/13项零值/);assert.doesNotMatch(await zero.locator('.orders-result summary').innerText(),/未记录/);await zero.locator('.orders-result summary').click();assert.equal(await zero.locator('.orders-result-full .orders-metric').count(),13);await zero.locator('.orders-result summary').click();assert.match(await p.locator('#ordersBody tr').nth(2).locator('.orders-results').innerText(),/数量未记录/);
 // Hit-test the fixed action before/after real horizontal wheel scrolling.
 if(width>600){await row.scrollIntoViewIfNeeded();const bounds=await wrap.boundingBox();await p.mouse.move(bounds.x+bounds.width/2,bounds.y+30);await p.mouse.wheel(800,0);await p.waitForTimeout(150);if(width===1024)assert.ok(await wrap.evaluate(w=>w.scrollLeft>0));await wrap.evaluate(w=>w.scrollLeft=w.scrollWidth);await p.screenshot({path:path.join(artifacts,'scrolled-'+width+'.png')});}
 const action=row.locator('.orders-actions button');await action.scrollIntoViewIfNeeded();const box=await action.boundingBox();assert.ok(box.x>=0&&box.x+box.width<=width+1,JSON.stringify(box));assert.ok(await action.evaluate(b=>{const r=b.getBoundingClientRect();return b.contains(document.elementFromPoint(r.x+r.width/2,r.y+r.height/2));}));await action.click();await p.locator('#orderDetailBody').getByText(/本期记录工时/).waitFor();await p.evaluate(()=>closeOrderDetail());
 const right=await wrap.evaluate(w=>({left:w.scrollLeft,max:w.scrollWidth-w.clientWidth}));if(width===1024)assert.ok(right.max>100&&right.left>100,JSON.stringify(right));
 await p.locator('#tab-orders button').filter({hasText:'下一页'}).click();await p.waitForFunction(()=>_ordersOffset===200&&_ordersItems.length===5);await p.locator('#ordersBody .orders-actions button').last().click();await p.locator('#orderDetailBody').getByText(/本期记录工时/).waitFor();await p.evaluate(()=>closeOrderDetail());await p.locator('#tab-orders button').filter({hasText:'上一页'}).click();await p.waitForFunction(()=>_ordersOffset===0&&_ordersItems.length===200);
 // Existing query/filter controls still own the request and pagination state.
 await p.locator('#ordersFilterJobType').selectOption('qc');await p.locator('#tab-orders button').filter({hasText:/^查询$/}).click();await p.waitForFunction(()=>_ordersResult?.total===1);await p.locator('#ordersFilterJobType').selectOption('');await query(p);
 const outputs=await exports(p,String(width));
 // The legacy endpoint stamps its response wall clock even when as_of is supplied.
 // Compare every field, normalizing only that volatile legacy metadata column.
 const normalize=(name,text)=>name==='history'?text.replace(/^(按建单日期[^,\r\n]*,[^,\r\n]*,)[^,\r\n]*/gm,'$1<response-cutoff>'):text;
 const digest=text=>createHash('sha256').update(text).digest('hex');
 for(const name of Object.keys(baseline))assert.equal(digest(normalize(name,outputs[name])),digest(normalize(name,baseline[name])),name+' export differs from baseline at '+width);
 evidence.push({...collapsed,horizontal:right,exports:Object.fromEntries(Object.entries(outputs).map(([k,v])=>[k,{bytes:Buffer.byteLength(v),identicalToBaseline:true,...(k==='history'?{normalizedColumn:'截止时间（响应时间）'}:{})}]))});assert.deepEqual(errors,[]);console.log('PASS orders layout, expansion, paging, detail, filters and four CSV comparisons at '+width);
 }finally{await context.close();}}
 assert.equal(JSON.stringify(DB.raw.prepare('SELECT * FROM v2_ops_job_results ORDER BY id').all()),before);fs.writeFileSync(path.join(artifacts,'evidence.json'),JSON.stringify(evidence,null,2));
}finally{await browser.close();await new Promise(r=>server.close(r));DB.raw.close();}
