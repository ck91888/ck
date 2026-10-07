// Built 002 navigation + real staging Worker against fictional, in-memory data only.
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const fs=require('node:fs'),assert=require('node:assert/strict');
const origin='https://127.0.0.1:'+(process.env.CK_COURIER_FIXTURE_PORT||7852);
const staff=JSON.parse(fs.readFileSync('/tmp/ck-courier-browser-staff.json'));
const dir=process.env.CK_COURIER_DETAIL_OUTPUT||'/tmp/ck-courier-detail-review';fs.mkdirSync(dir,{recursive:true});
(async()=>{
 const browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});
 const errors=[],results=[];let batches;
 try{
  for(const [width,height,lang,suffix] of [[1365,900,'zh','desktop-zh'],[1365,900,'ko','desktop-ko'],[390,844,'zh','mobile-zh'],[360,720,'ko','mobile-ko']]){
   const c=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width,height}});
   await c.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
   let mode=null,release=null,held=null;
   await c.route('**/api',async route=>{
    if(new URL(route.request().url()).origin!==origin)return route.abort();
    const v=route.request().postDataJSON(),m=mode;
    if(!m||v.action!==m.action||(v.batch_id||v.id)!==m.id)return route.continue();
    if(m.kind==='failure')return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'隔离测试：服务暂不可用 / 테스트: 잠시 사용할 수 없음'})});
    if(m.kind==='mismatch')return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:true,items:[],total:2,limit:50,offset:0})});
    if(m.kind==='hold'){
     const response=await route.fetch();held?.();await new Promise(resolve=>release=resolve);await route.fulfill({response});m.delivered?.();return;
    }
    return route.continue();
   });
   const p=await c.newPage();p.on('pageerror',e=>errors.push(e.message));
   await p.goto(origin+'/');await p.locator('#ck-login-code').fill('local-browser-fixture-only');await p.locator('.ck-login-submit').click();await p.locator('.ck-home-grid').waitFor();
   if(!batches)batches=await p.evaluate(async staff=>{
    const out=[];
    for(const codes of [[['601000000101','8-1'],['601000000102','8-4']],[['JJD123456789012345678','tent']],[],Array.from({length:51},(_,i)=>['601000000'+(200+i),'8-3'])]){
     const r=await CKSession.request('sop_courier_receive',{operation:'start',workers:staff.slice(0,2),lead_id:staff[0].id,client_req_id:crypto.randomUUID()});
     for(const [tracking_no,owner]of codes)await CKSession.request('sop_courier_receive',{batch_id:r.batch.id,scanner_badge:staff[0].id,tracking_no,owner});
     out.push((await CKSession.request('sop_courier_receive',{operation:'finish',batch_id:r.batch.id,revision:r.batch.revision,client_req_id:crypto.randomUUID()})).batch);
    }
    return out;
   },staff);
   await p.locator('.ck-home-grid a[href="/002/"]').click();await p.locator('#mainTabs [data-tab=courier]').waitFor();
   await p.evaluate(lang=>{setLang(lang);applyLang();},lang);await p.locator('#mainTabs [data-tab=courier]').click();
   const view=p.locator('#view-courier'),d=p.locator('dialog.courier-batch-detail[open]');
   const open=async index=>{await view.locator('[data-batch="'+batches[index].id+'"]').click();};
   const close=async()=>{await d.locator('.courier-dialog-head button').click();await d.waitFor({state:'hidden'});};
   const ready=async n=>{await p.waitForFunction(n=>{const x=document.querySelector('dialog.courier-batch-detail[open]');return x&&!x.hasAttribute('aria-busy')&&x.querySelectorAll('tbody [data-detail]').length===n;},n);};
   await view.locator('[data-batch]').first().waitFor();await open(0);await ready(2);
   const geometry=await d.evaluate(el=>{const r=el.getBoundingClientRect();return {left:r.left,top:r.top,width:r.width,height:r.height,viewport:innerWidth,overflow:el.scrollWidth>el.clientWidth};});
   assert.ok(Math.abs(geometry.left-(width-geometry.width)/2)<2);assert.ok(geometry.top>0);assert.equal(geometry.overflow,false);
   assert.equal(await d.locator('h3').textContent(),batches[0].batch_no);assert.match(await d.locator('[data-summary]').textContent(),/2 件/);
   assert.match(await d.locator('.courier-batch-meta').textContent(),/派工人.*管理员.*主操作员.*隔离派工员.*参与人员.*隔离派工员、隔离扫描员/s);
   assert.match(await d.locator('.courier-batch-state').textContent(),/已完成/);
   assert.deepEqual(await d.locator('.courier-batch-counts .has-receipts').allTextContents(),['8-11','8-41']);
   assert.deepEqual(await d.locator('.courier-waybill').allTextContents(),['601000000102','601000000101']);
   assert.equal(await d.locator('[data-pages]').isVisible(),false);assert.match(await d.locator('.courier-pager span').textContent(),/2 件.*1–2.*1 \/ 1/);
   await d.screenshot({path:dir+'/after-two-receipts-'+suffix+'.png'});
   if(width<600){
    await d.evaluate(el=>el.scrollTop=el.scrollHeight);
    const shown=await d.evaluate(el=>{const r=el.getBoundingClientRect();return [...el.querySelectorAll('tbody tr')].map(tr=>{const t=tr.getBoundingClientRect();return {text:tr.textContent,visible:t.top>=r.top&&t.bottom<=r.bottom};});});
    assert.equal(shown.length,2);assert.ok(shown.every(x=>x.visible));
    await d.screenshot({path:dir+'/after-two-receipts-scrolled-'+suffix+'.png'});await d.evaluate(el=>el.scrollTop=0);
   }
   await d.locator('[data-detail]').first().click();const receipt=p.locator('dialog.courier-receipt-detail[open]');await receipt.waitFor();
   assert.equal(await receipt.locator('[name=tracking]').inputValue(),'601000000102');assert.equal(await receipt.locator('[data-owner="8-4"]').getAttribute('aria-pressed'),'true');
   const ownerStyle=await receipt.locator('.courier-owner-buttons').evaluate(el=>{const selected=getComputedStyle(el.querySelector('[aria-pressed=true]')),other=getComputedStyle(el.querySelector('[aria-pressed=false]'));return {selected:selected.backgroundColor,other:other.backgroundColor};});
   assert.notEqual(ownerStyle.selected,ownerStyle.other);assert.equal(ownerStyle.other,'rgb(255, 255, 255)');
   assert.equal(await receipt.locator('[data-save]').isVisible(),true);assert.equal(await receipt.locator('details summary').isVisible(),true);
   const rect=await receipt.boundingBox();assert.ok(Math.abs(rect.x-(width-rect.width)/2)<2);
   await receipt.screenshot({path:dir+'/after-receipt-editor-'+suffix+'.png'});
   await receipt.locator('.courier-dialog-head button').click();assert.equal(await d.isVisible(),true);assert.equal(await d.locator('tbody [data-detail]').count(),2);
   await d.screenshot({path:dir+'/after-return-from-receipt-'+suffix+'.png'});await close();
   // Slow real response: a visible loading state, then the real two rows.
   mode={kind:'hold',action:'sop_courier_list',id:batches[0].id};const pending=new Promise(resolve=>held=resolve);
   await open(0);await pending;await d.locator('[role=status]').filter({hasText:'正在加载'}).waitFor();assert.equal(await d.getAttribute('aria-busy'),'true');
   assert.equal(await d.locator('tbody [data-detail]').count(),0);assert.equal(await d.locator('.courier-pager').isVisible(),false);
   await d.screenshot({path:dir+'/after-loading-'+suffix+'.png'});mode=null;release();await ready(2);assert.equal(await d.locator('[role=status]').textContent(),'');await close();
   // Failure is distinct from empty. Retry uses the unchanged batch scope.
   mode={kind:'failure',action:'sop_courier_list',id:batches[0].id};await open(0);await d.locator('[data-retry]').waitFor({state:'visible'});
   assert.match(await d.locator('[role=status]').textContent(),/读取失败.*服务暂不可用/s);assert.equal(await d.locator('tbody [data-detail]').count(),0);
   await d.screenshot({path:dir+'/after-error-'+suffix+'.png'});mode=null;await d.locator('[data-retry]').click();await ready(2);assert.equal(await d.locator('[data-retry]').isVisible(),false);
   await d.screenshot({path:dir+'/after-retry-restored-'+suffix+'.png'});await close();
   mode={kind:'failure',action:'sop_courier_detail',id:batches[0].id};await open(0);await d.locator('[data-retry]').waitFor({state:'visible'});mode=null;await d.locator('[data-retry]').click();await ready(2);await close();
   mode={kind:'mismatch',action:'sop_courier_list',id:batches[0].id};await open(0);await d.locator('[data-retry]').waitFor({state:'visible'});assert.match(await d.locator('[role=status]').textContent(),/件数与明细不一致/);mode=null;await close();
   await open(2);await ready(0);assert.match(await d.locator('tbody').textContent(),/本批暂无收货记录/);assert.equal(await d.locator('[data-retry]').isVisible(),false);
   if(width<600)assert.deepEqual(await d.locator('td.courier-empty').evaluate(x=>{const s=getComputedStyle(x);return [s.gridColumnStart,s.gridColumnEnd,s.gridRowStart,s.textAlign];}),['1','-1','auto','center']);
   await d.screenshot({path:dir+'/after-empty-'+suffix+'.png'});await close();
   // Long supported JJD barcode, no clipping, plus isolation between batches.
   await open(1);await ready(1);assert.equal(await d.locator('.courier-waybill').textContent(),'JJD123456789012345678');assert.doesNotMatch(await d.locator('tbody').textContent(),/60100000010/);
   assert.equal(await d.evaluate(x=>x.scrollWidth>x.clientWidth),false);await d.screenshot({path:dir+'/after-long-waybill-'+suffix+'.png'});await close();
   // Late response after closing cannot overwrite a different batch.
   mode={kind:'hold',action:'sop_courier_list',id:batches[0].id};const oldPending=new Promise(resolve=>held=resolve);await open(0);await oldPending;await close();mode=null;await open(1);await ready(1);release();
   await p.waitForFunction(()=>document.querySelector('dialog.courier-batch-detail[open] tbody [data-detail]')?.closest('tr').textContent.includes('JJD'));
   assert.equal(await d.locator('h3').textContent(),batches[1].batch_no);assert.equal(await p.locator('dialog.courier-batch-detail').count(),1);await close();
   // A pending receipt belongs to its parent batch; close/switch must not reopen it.
   await open(0);await ready(2);const receiptId=await d.locator('[data-detail]').first().getAttribute('data-detail');
   let delivered;const receiptDelivered=new Promise(resolve=>delivered=resolve);mode={kind:'hold',action:'sop_courier_detail',id:receiptId,delivered};
   const receiptPending=new Promise(resolve=>held=resolve);await d.locator('[data-detail]').first().click();await receiptPending;
   assert.match(await d.locator('[role=status]').textContent(),/正在读取单票/);assert.equal(await d.locator('[data-detail]').first().isDisabled(),true);
   await close();mode=null;await open(1);await ready(1);release();await receiptDelivered;await p.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
   assert.equal(await p.locator('dialog.courier-receipt-detail[open]').count(),0);assert.equal(await d.locator('h3').textContent(),batches[1].batch_no);await close();
   await open(3);await ready(50);assert.equal(await d.locator('[data-pages]').isVisible(),true);assert.equal(await d.locator('[data-prev]').isDisabled(),true);assert.equal(await d.locator('[data-next]').isDisabled(),false);
   assert.match(await d.locator('.courier-pager span').textContent(),/51 件.*1–50.*1 \/ 2/);await d.locator('[data-next]').click();await ready(1);
   assert.match(await d.locator('.courier-pager span').textContent(),/51 件.*51–51.*2 \/ 2/);assert.equal(await d.locator('[data-next]').isDisabled(),true);assert.equal(await d.locator('[data-prev]').isDisabled(),false);
   await d.screenshot({path:dir+'/after-pagination-'+suffix+'.png'});await d.locator('[data-prev]').click();await ready(50);await close();
   // Existing office query and underlying scroll survive closing the matching detail.
   const input=view.locator('[name=keyword]');await input.fill('601000000101');await view.locator('form button:not([type=button])').click();await view.locator('[role=status]').filter({hasText:'匹配 1 批'}).waitFor();
   await open(0);await ready(1);assert.equal(await d.locator('mark').textContent(),'601000000101');const scroll=await p.evaluate(()=>scrollY);await close();assert.equal(await p.evaluate(()=>scrollY),scroll);assert.equal(await input.inputValue(),'601000000101');assert.equal(await view.locator('[data-batch]').count(),1);
   await input.fill('301000009999');await view.locator('form button:not([type=button])').click();await view.locator('[data-history]').filter({hasText:'1 件'}).waitFor();await view.locator('[data-history]').click();await ready(1);assert.equal(await d.locator('mark').textContent(),'301000009999');await close();
   if(suffix==='desktop-zh'){
    // An unanswered request becomes retryable at the local 15-second boundary.
    await view.locator('[data-clear]').click();await view.locator('[data-batch]').first().waitFor();mode={kind:'hold',action:'sop_courier_list',id:batches[0].id};const timeoutHeld=new Promise(resolve=>held=resolve);await open(0);await timeoutHeld;
    await d.locator('[data-retry]').waitFor({state:'visible',timeout:20000});assert.match(await d.locator('[role=status]').textContent(),/读取超时/);mode=null;await d.locator('[data-retry]').click();await ready(2);release();await close();
   }
   assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);results.push({viewport:width+'x'+height,lang,geometry,ok:true});await c.close();
  }
  assert.deepEqual(errors,[]);const report={ok:true,results,pageErrors:errors,scenarios:['actual home /002/ courier navigation','completed two-receipt batch with roles and owner groups','Chinese/Korean desktop/mobile','centered batch and existing receipt editor','mobile scroll shows both receipts','receipt return preserves batch','slow real response','list/detail failure and retry','inconsistent count rejects false empty','true empty spans full mobile card','long supported JJD waybill','close/switch late batch and receipt response isolation','51 receipt pagination and bounds','search and legacy query/scroll preserved','15-second timeout and retry'],batchIds:batches.map(x=>x.id),screenshots:fs.readdirSync(dir).filter(x=>x.startsWith('after-')&&x.endsWith('.png'))};
  fs.writeFileSync(dir+'/detail-acceptance.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
