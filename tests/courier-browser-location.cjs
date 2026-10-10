// Actual navigation, built pages and Worker; fixture data only, all other origins blocked.
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const fs=require('node:fs'),assert=require('node:assert/strict');
const origin='https://127.0.0.1:'+(process.env.CK_COURIER_FIXTURE_PORT||7833);
const staff=JSON.parse(fs.readFileSync('/tmp/ck-courier-browser-staff.json'));
const dir='/tmp/ck-courier-ui-correction';fs.mkdirSync(dir,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 const errors=[],results=[];let batch;
 try{
  for(const [width,height,lang,suffix] of [[1365,900,'zh','desktop-zh'],[1365,900,'ko','desktop-ko'],[390,844,'zh','mobile-zh'],[360,720,'ko','mobile-ko']]){
   const c=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width,height}});
   await c.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
   const p=await c.newPage();p.on('pageerror',e=>errors.push(e.message));
   await p.goto(origin+'/');await p.locator('#ck-login-code').fill('local-browser-fixture-only');await p.locator('.ck-login-submit').click();await p.locator('#ck-operator-name').waitFor();await p.locator('#ck-operator-name').fill('隔离浏览器测试员');await p.locator('[data-operator-confirm]').click();
   await p.locator('.ck-home-grid').waitFor();
   assert.deepEqual(await p.locator('.ck-home-grid a').evaluateAll(a=>a.map(x=>x.getAttribute('href'))),['/001/','/attendance/','/002/','/003/','/shuju/']);
   assert.equal(await p.locator('.courier-field,input[name=tracking]').count(),0);
   assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
   await p.screenshot({path:dir+'/workbench-'+suffix+'.png',fullPage:true});
   if(!batch)batch=await p.evaluate(async staff=>{
    const r=await CKSession.request('sop_courier_receive',{operation:'start',workers:staff.slice(0,2),lead_id:staff[0].id,client_req_id:crypto.randomUUID()});
    for(const code of ['401000000201',...Array.from({length:52},(_,i)=>'401000000'+(210+i))])await CKSession.request('sop_courier_receive',{batch_id:r.batch.id,scanner_badge:staff[0].id,owner:'8-1',tracking_no:code});
    return r.batch;
   },staff);
   await p.locator('.ck-home-grid a[href="/002/"]').click();await p.locator('#mainTabs [data-tab=courier]').waitFor();
   await p.evaluate(lang=>{setLang(lang);applyLang();},lang);await p.locator('#mainTabs [data-tab=courier]').click();
   const view=p.locator('#view-courier'),input=view.locator('[name=keyword]'),submit=view.locator('form button:not([type=button])');
   await view.locator('[data-batch]').first().waitFor();assert.equal(await view.locator('input[type=search]').count(),1);
   await input.fill('401000000201');await submit.click();
   await view.locator('[role=status]').filter({hasText:'匹配 1 批'}).waitFor();
   assert.equal(await view.locator('[data-batch]').count(),1);assert.match(await view.locator('tbody').textContent(),new RegExp(batch.batch_no));
   await view.screenshot({path:dir+'/batch-search-'+suffix+'.png'});await view.locator('[data-batch]').click();
   const d=p.locator('dialog[open]');await d.locator('mark').waitFor();assert.equal(await d.locator('mark').textContent(),'401000000201');
   assert.equal(await d.locator('tbody [data-detail]').count(),1);assert.match(await d.locator('[data-summary]').textContent(),/53 件/);
   await d.screenshot({path:dir+'/matching-waybill-'+suffix+'.png'});
   await d.locator('[data-all]').click();await p.waitForFunction(()=>document.querySelector('dialog[open] [data-next]')?.disabled===false);
   assert.equal(await d.locator('tbody [data-detail]').count(),50);await d.locator('[data-next]').click();
   await p.waitForFunction(()=>document.querySelector('dialog[open] tbody')?.querySelectorAll('[data-detail]').length===3);
   assert.equal(await d.locator('mark').textContent(),'401000000201'); // Original receipt is beyond the unfiltered first page.
   await d.locator('[data-all]').click();await p.waitForFunction(()=>document.querySelector('dialog[open] tbody')?.querySelectorAll('[data-detail]').length===1);
   await d.locator('.courier-dialog-head button').click();
   assert.equal(await input.inputValue(),'401000000201');await view.locator('[data-clear]').click();
   await p.waitForFunction(()=>document.querySelector('#view-courier [role=status]')?.textContent==='');assert.equal(await input.inputValue(),'');
   assert.notEqual(await view.locator('[name=from]').inputValue(),'');assert.equal(await view.locator('.courier-matched-batch').count(),0);
   await input.fill('999000000001');await submit.click();await view.locator('[role=status]').filter({hasText:'未找到该运单'}).waitFor();
   assert.equal(await view.locator('[data-batch]').count(),0);
   await input.fill('301000009999');await submit.click();await view.locator('[data-history]').filter({hasText:'1 件'}).waitFor();
   assert.equal(await view.locator('[data-batch]').count(),0);await view.locator('[data-history]').click();
   await d.locator('mark').waitFor();assert.match(await d.locator('h3').textContent(),/历史收货（无批次）/);
   assert.equal(await d.locator('mark').textContent(),'301000009999');assert.equal(await d.locator('[data-all]').isVisible(),false);
   await d.screenshot({path:dir+'/history-search-'+suffix+'.png'});await d.locator('.courier-dialog-head button').click();
   await input.fill('');await p.waitForFunction(()=>document.querySelector('#view-courier [role=status]')?.textContent==='');
   await view.locator('[data-batch]').first().waitFor();assert.equal(await view.locator('input[type=search]').count(),1);
   assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
   await p.locator('.ck-cross-nav a[href="/"]').click();await p.locator('.ck-home-grid').waitFor();assert.equal(await p.locator('.courier-field').count(),0);
   await p.locator('.ck-home-grid a[href="/002/"]').click();await p.locator('#mainTabs [data-tab=courier]').click();
   await view.locator('[data-batch]').first().waitFor();assert.equal(await input.inputValue(),'');
   results.push({viewport:width+'x'+height,lang,ok:true});await c.close();
  }
  assert.deepEqual(errors,[]);const report={ok:true,results,pageErrors:errors,scenarios:['workbench restored','real coordination navigation','single integrated search','correct batch','matching waybill beyond first unfiltered page','all/matching detail toggle and pagination','clear filters','no result','legacy without batch','return and re-enter'],screenshots:fs.readdirSync(dir).filter(x=>x.endsWith('.png'))};
  fs.writeFileSync(dir+'/navigation-acceptance.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
