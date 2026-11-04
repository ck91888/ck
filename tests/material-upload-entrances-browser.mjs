// Built UI + authenticated Worker. Loopback, synthetic files, no remote business data.
import assert from 'node:assert/strict';import {createRequire} from 'node:module';
import {materialUploadFixture,multipartField} from './multiple-material-upload-fixture.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),f=await materialUploadFixture(),errors=[];
const selections=Array.from({length:6},(_,i)=>({name:'synthetic-'+i+'.csv',mimeType:'text/csv',buffer:Buffer.from('synthetic row '+i)}));
try{for(const width of [1440,390])for(const mode of ['ordinary','batch','group','bulk']){
 const plan=await f.call('v2_inbound_plan_create',{customer:'合成上传入口',biz_classes:['bulk']}),n=await f.call('sop_need_create',{department:'bulk',source_type:'inbound',source_id:plan.id,title:'合成多文件 '+mode,instructions:'隔离验证',reason:'合成添加作业'}),need={id:n.id,status:'pending'},context=await browser.newContext({viewport:{width,height:1000}});
 await context.addCookies([{name:'ck_office_access',value:f.cookie.split('=')[1],url:f.origin}]);const page=await context.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.accept());await page.route('**/*',r=>new URL(r.request().url()).origin===f.origin?r.continue():r.abort());
 await page.goto(f.origin+'/002/');await page.waitForFunction(()=>window.CKSession?.user?.operation_context?.id);await page.evaluate(async({mode,need,source})=>{const host=document.createElement('section');host.id='synthetic-upload-case';document.body.append(host);if(mode==='ordinary')await CKWorkChain.materials(host,need);else if(mode==='batch')await CKWorkChain.batchMaterials(host,{source_type:'inbound',source_id:source,items:[need]});else await CKCargoGroups.mount(host,need);},{mode,need,source:plan.id});const host=page.locator('#synthetic-upload-case');
 const attempts=[];let disconnects=0,active=0,peak=0;const permanent=mode==='batch';
 await page.route('**/api',async route=>{const req=route.request();if(!multipartField(req,'file')&&!req.postDataBuffer()?.includes(Buffer.from('filename="')))return route.continue();active++;peak=Math.max(peak,active);const key=multipartField(req,'client_req_id'),filename=req.postDataBuffer().toString().match(/filename="([^"]+)"/)?.[1];attempts.push({key,filename});try{
  if(filename==='synthetic-2.csv'&&disconnects<(permanent?3:1)){disconnects++;if(!permanent)await route.fetch();await route.abort('failed');return;}
  await new Promise(r=>setTimeout(r,40));await route.continue();
 }finally{active--;}});
 let submit,status,input;
 if(mode==='ordinary'){const form=host.locator('[data-direction=office] form');input=form.locator('[data-files]');submit=form.locator('[type=submit]');status=form.locator('[data-status]');}
 else if(mode==='batch'){input=host.locator('form input[type=file]');submit=host.locator('form button');status=host.locator('form [role=status]');}
 else if(mode==='bulk'){await host.locator('[data-cargo-advanced] > summary').click();input=host.locator('[data-upload-files]');submit=host.locator('.chain-upload button');status=host.locator('[data-upload-status]');}
 else{await host.locator('[data-add]').click();const g=host.locator('.cargo-group-editor');await g.locator('[data-range]').fill('SYN001-SYN006');await g.locator('[data-count]').fill('6');input=g.locator('[data-group-upload]');submit=host.locator('[data-editor-save] [type=submit]');status=host.locator('[data-status]').first();}
 await input.setInputFiles(selections);await submit.click();
 if(permanent){await status.filter({hasText:'已确认文件保留'}).waitFor();assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_attachments WHERE related_doc_id=?").get(plan.id).n,2);await submit.click();}
 if(mode==='ordinary')await host.locator('[data-direction=office] .chain-files tbody tr').nth(5).waitFor();
 else if(mode==='batch')await host.locator('[data-batch-files] .chain-files tbody tr').nth(5).waitFor();
 else if(mode==='bulk')await status.filter({hasText:'已上传'}).waitFor();
 else await status.filter({hasText:'已保存'}).waitFor();
 const id=mode==='batch'?plan.id:n.id,rows=f.DB.raw.prepare('SELECT * FROM v2_attachments WHERE related_doc_id=?').all(id);assert.equal(rows.length,6);assert.equal(new Set(attempts.map(x=>x.key)).size,6);assert.equal(attempts.length,permanent?9:7);assert.equal(peak,1,'saved need revisions must upload sequentially');for(let i=0;i<6;i++)assert.equal(rows.filter(x=>x.file_name===selections[i].name).length,1);
 if(mode==='group'){const saved=JSON.parse(f.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(n.id).state);assert.equal(saved.cargo_groups.groups[0].documents.length,6);assert.deepEqual(saved.cargo_groups.groups[0].documents.map(x=>x.file_name),selections.map(x=>x.name));}
 console.log('PASS '+JSON.stringify({width,mode,requests:attempts.length,uniqueKeys:6,attachments:6,peak,recovery:permanent?'manual continuation':'lost acknowledgement replay'}));await context.close();
}assert.deepEqual(errors,[]);}finally{await browser.close();await f.close();}
