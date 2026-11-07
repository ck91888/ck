// Native shared dispatch component, cancellation and completion; isolated fixture only.
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const fs=require('node:fs'),assert=require('node:assert/strict');
const origin='https://127.0.0.1:'+(process.env.CK_COURIER_FIXTURE_PORT||7833),staff=JSON.parse(fs.readFileSync('/tmp/ck-courier-browser-staff.json'));
const dir='/tmp/ck-courier-ui-correction';fs.mkdirSync(dir,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),errors=[],results=[];
 try{for(const [width,height,lang,suffix]of [[1365,900,'zh','desktop-zh'],[1365,900,'ko','desktop-ko'],[390,844,'zh','mobile-zh'],[360,720,'ko','mobile-ko']]){
  const c=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width,height}});await c.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
  const p=await c.newPage();p.on('pageerror',e=>errors.push(e.message));let starts=0,finishes=0;
  p.on('request',r=>{if(r.method()==='POST')try{const b=JSON.parse(r.postData());if(b.action==='sop_courier_receive'&&b.operation==='start')starts++;if(b.action==='sop_courier_receive'&&b.operation==='finish')finishes++;}catch{}});
  await p.goto(origin+'/001/');await p.locator('#ck-login-code').fill(staff[0].id+'|'+staff[0].name);await p.locator('.ck-login-submit').click();await p.locator('#page-home.active').waitFor();await p.evaluate(lang=>{window.setLang?.(lang);window.applyLang?.();},lang);
  const entry=p.locator('#page-home .home-btn').filter({hasText:'快递收货'});await entry.click();
  const d=p.locator('dialog.courier-dispatch[open]');await d.waitFor();
  assert.equal(await d.locator('[data-staff-badge]').inputValue(),'');assert.equal(await d.locator('[data-staff-badge]').getAttribute('placeholder'),'工号|姓名 / 사번|이름');assert.equal(await d.locator('[type=submit]').isDisabled(),true);
  const geometry=await d.evaluate(el=>{const r=el.getBoundingClientRect();return {left:r.left,right:innerWidth-r.right,top:r.top,bottom:innerHeight-r.bottom,overflow:el.scrollWidth>el.clientWidth,primary:getComputedStyle(el.querySelector('[type=submit]')).backgroundColor};});
  assert.ok(Math.abs(geometry.left-geometry.right)<2,JSON.stringify(geometry));assert.ok(geometry.top>=0&&geometry.bottom>=0);assert.equal(geometry.overflow,false);
  assert.equal(await d.locator('button').evaluateAll(a=>a.every(x=>x.getBoundingClientRect().height>=44)),true);
  await p.screenshot({path:dir+'/dispatch-after-empty-'+suffix+'.png',fullPage:true});
  await d.locator('[data-cancel]').click();assert.equal(starts,0);await p.evaluate(()=>{CKChooseNativeStaff({action:'v2_unload_job_start'});});
  const native=p.locator('dialog.ck-workflow[open]');await native.waitFor();assert.equal(await native.locator('[type=submit]').evaluate(el=>getComputedStyle(el).backgroundColor),geometry.primary);
  await native.screenshot({path:dir+'/native-shared-after-'+suffix+'.png'});await native.locator('[data-cancel]').click();
  await p.locator('[data-new]').click();await p.keyboard.press('Escape');await d.waitFor({state:'hidden'});await p.locator('[data-new]').dblclick();assert.equal(await p.locator('dialog[open]').count(),1);
  async function add(i){await d.locator('[data-staff-badge]').fill(staff[i].id+'|'+staff[i].name);await d.locator('[data-staff-add]').click();}
  await add(0);await add(0);assert.equal(await d.locator('[data-staff-people] .badge').count(),1);assert.match(await d.locator('[data-staff-status]').textContent(),/未重复加入/);await add(1);
  await d.locator('[data-staff-people] .badge button').first().click();assert.equal(await d.locator('[type=submit]').isDisabled(),true);await d.locator('[data-staff-lead]').selectOption(staff[1].id);assert.equal(await d.locator('[type=submit]').isEnabled(),true);
  await add(0);await d.locator('[data-staff-lead]').selectOption(staff[0].id);await p.screenshot({path:dir+'/dispatch-after-people-'+suffix+'.png',fullPage:true});
  await d.locator('[type=submit]').click();await p.locator('[data-batch-title]').waitFor();assert.equal(starts,1);
  const batchId=await p.evaluate(async()=>{const r=await CKSession.request('sop_courier_config',{operation:'batches'});return r.items.find(x=>x.status==='working').id;});
  await p.locator('[data-people]').click();await d.waitFor();await d.locator('[data-staff-people] .badge button').first().click();await d.locator('[data-cancel]').click();assert.match(await p.locator('[data-crew]').textContent(),new RegExp(staff[0].name));
  p.once('dialog',x=>x.dismiss());await p.locator('[data-finish]').click();assert.equal(await p.locator('#page-courier').evaluate(el=>el.classList.contains('active')),true);assert.equal(finishes,0);
  if(suffix==='desktop-zh'){
   const reject=r=>{const b=r.request().postDataJSON();return b?.operation==='finish'?r.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:false,error:'隔离测试：完成被拒绝'})}):r.continue();};
   await c.route('**/001/api',reject);p.once('dialog',x=>x.accept());await p.locator('[data-finish]').click();await p.locator('[data-feedback]').filter({hasText:'完成被拒绝'}).waitFor();assert.equal(await p.locator('#page-courier.active').count(),1);await c.unroute('**/001/api',reject);
   await c.setOffline(true);p.once('dialog',x=>x.accept());await p.locator('[data-finish]').click();await p.locator('[data-feedback]').filter({hasText:'完成未确认'}).waitFor();assert.equal(await p.locator('#page-courier.active').count(),1);await c.setOffline(false);
   await p.evaluate(()=>{const original=CKSession.request;CKSession.request=async(a,b)=>{const r=await original(a,b);if(a==='sop_courier_receive'&&b.operation==='finish')throw new TypeError('隔离测试：已提交完成响应丢失');return r;};});
  }
  const before=finishes;p.once('dialog',x=>x.accept());await p.evaluate(()=>{document.querySelector('[data-finish]').click();document.querySelector('[data-finish]').click();});await p.locator('#page-home.active').waitFor();assert.equal(finishes,before+1);
  assert.equal(await p.locator('[data-courier-host]').textContent(),'');assert.equal(await p.evaluate(()=>_navStack.length),0);assert.equal(await p.evaluate(()=>_currentPage),'home');
  await p.screenshot({path:dir+'/completed-field-home-'+suffix+'.png',fullPage:true});
  const completed=await p.evaluate(async id=>(await CKSession.request('sop_courier_detail',{batch_id:id})).batch,batchId);assert.equal(completed.status,'completed');assert.equal(completed.workers.length,0);
  await p.evaluate(()=>goBack());assert.equal(await p.locator('#page-home.active').count(),1);await p.reload();await p.locator('#page-home.active').waitFor();assert.equal(await p.locator('[data-courier-host] input').count(),0);
  await p.goto(origin+'/001/?tab=courier');await p.locator('dialog [data-staff-badge]').waitFor();await p.evaluate(id=>{CKOpenCourierBatch(id);},batchId);await p.locator('#page-home.active').waitFor();assert.equal(new URL(p.url()).searchParams.has('tab'),false);
  await p.goBack();await p.locator('#page-home.active').waitFor();assert.equal(await p.locator('[data-courier-host] input').count(),0);
  results.push({viewport:width+'x'+height,lang,geometry,ok:true});await c.close();
 }
 assert.deepEqual(errors,[]);const report={ok:true,results,pageErrors:errors,scenarios:['same native dispatch component','neutral empty badge','zero people disabled','centered responsive dialog','44px controls','add duplicate/remove/change lead','visible feedback','cancel/Escape/reopen','no API on cancel','adjust cancellation preserves crew','cancel completion stays','failed completion stays','offline completion stays','committed lost response verified before exit','double completion one request','confirmed completion goes to field home','cleared UI and navigation','reload and browser back cannot reopen completed scanning']};fs.writeFileSync(dir+'/dispatch-completion-acceptance.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
