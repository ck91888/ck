// Actual XLSX downloads from built navigation + real Worker. Fictional fixture only.
const {chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const fs=require('node:fs'),assert=require('node:assert/strict'),XLSX=require('../shared/xlsx.full.min.js');
const origin='https://127.0.0.1:'+(process.env.CK_COURIER_FIXTURE_PORT||7862),fixture=JSON.parse(fs.readFileSync('/tmp/ck-courier-export-fixture.json'));
const dir='/tmp/ck-courier-export-review';fs.mkdirSync(dir,{recursive:true});
(async()=>{
 const b=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']});const errors=[],results=[];let correctionId;
 try{
  let n=0;
  for(const [width,height,lang,suffix]of [[1365,900,'zh','desktop-zh'],[1365,900,'ko','desktop-ko'],[390,844,'zh','mobile-zh'],[360,720,'ko','mobile-ko']]){
   const c=await b.newContext({ignoreHTTPSErrors:true,acceptDownloads:true,viewport:{width,height}});await c.route('**/*',r=>new URL(r.request().url()).origin===origin?r.continue():r.abort());
   const p=await c.newPage(),downloads=[],calls=[];let mode=null,held=null,release=null;
   p.on('pageerror',e=>errors.push(e.message));p.on('download',d=>downloads.push(d));
   await c.route('**/api',async r=>{
    if(new URL(r.request().url()).origin!==origin)return r.abort();
    const v=r.request().postDataJSON();if(v.action!=='sop_courier_list'||v.mode!=='batch_export')return r.continue();calls.push(v);
    const m=mode;if(!m||(m.offset!==undefined&&v.offset!==m.offset)||(m.final&&!v.snapshot))return r.continue();
    if(m.kind==='failure')return r.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'隔离导出失败 / 테스트 내보내기 실패'})});
    if(m.kind==='change'){
     mode=null;await p.evaluate(async x=>{const r=await CKSession.request('sop_courier_detail',{id:x.id});await CKSession.request('sop_courier_update',{id:x.id,version:r.item.version,tracking_no:x.code,owner:r.item.owner});},{id:correctionId,code:m.code});return r.continue();
    }
    const response=await r.fetch();
    if(m.kind==='wrong-batch'){const x=await response.json();x.items[0].batch_id=fixture.batches[2].id;return r.fulfill({status:200,contentType:'application/json',body:JSON.stringify(x)});}
    if(m.kind==='hold'){held?.();await new Promise(resolve=>release=resolve);await r.fulfill({response});m.delivered?.();return;}
    return r.fulfill({response});
   });
   await p.goto(origin+'/');await p.locator('#ck-login-code').fill('local-browser-fixture-only');await p.locator('.ck-login-submit').click();await p.locator('#ck-operator-name').waitFor();await p.locator('#ck-operator-name').fill('隔离浏览器测试员');await p.locator('[data-operator-confirm]').click();await p.locator('.ck-home-grid').waitFor();
   await p.locator('.ck-home-grid a[href="/002/"]').click();await p.locator('#mainTabs [data-tab=courier]').waitFor();await p.evaluate(lang=>{setLang(lang);applyLang();},lang);await p.locator('#mainTabs [data-tab=courier]').click();
   const view=p.locator('#view-courier'),d=p.locator('dialog.courier-batch-detail[open]');
   const open=async index=>{await view.locator('[data-batch="'+fixture.batches[index].id+'"]').click();await d.locator('[data-summary] b').waitFor();await p.waitForFunction(()=>!document.querySelector('dialog.courier-batch-detail[open]')?.hasAttribute('aria-busy'));};
   const close=async()=>{await d.locator('.courier-dialog-head button').click();await d.waitFor({state:'hidden'});};
   const exportFile=async tag=>{
    const event=p.waitForEvent('download');await d.locator('[data-export]').click();const download=await event;assert.equal(await download.failure(),null);
    const file=dir+'/'+tag+'.xlsx';await download.saveAs(file);const wb=XLSX.read(fs.readFileSync(file),{type:'buffer',cellNF:true}),ws=wb.Sheets[wb.SheetNames[0]],rows=XLSX.utils.sheet_to_json(ws,{header:1,raw:true});
    assert.equal(rows.length,206);assert.equal(rows[0].length,5);assert.equal(new Set(rows.slice(1).map(x=>x[1])).size,205);assert.equal(rows.slice(1).every(x=>x[0]===fixture.batches[0].batch_no),true);
    const zero=rows.find(x=>x[1]==='0012345678901'),long=rows.find(x=>x[1]==='JJD123456789012345678');assert.ok(zero);assert.ok(long);assert.equal(zero[3],fixture.scanner);assert.equal(zero[4],'2026-10-07 23:59:59');assert.equal(long[4],'2026-10-08 00:00:00');
    assert.equal(rows.some(x=>x[1]==='799000000999'||x[1]==='301000009999'),false);
    for(const key of Object.keys(ws)){if(key.startsWith('!'))continue;assert.equal(ws[key].t,'s');assert.equal(ws[key].z,'@');assert.equal(ws[key].f,undefined);}
    assert.equal(download.suggestedFilename(),'CK-快递收货-'+fixture.batches[0].batch_no+'.xlsx');return {file,rows:rows.length-1,filename:download.suggestedFilename()};
   };
   await open(0);assert.equal(await d.locator('[data-export]').isEnabled(),true);assert.match(await d.locator('[data-export-scope]').textContent(),/本批全部/);
   if(!correctionId)correctionId=await p.evaluate(async id=>(await CKSession.request('sop_courier_list',{batch_id:id,keyword:'701000000201'})).items[0].id,fixture.batches[0].id);
   const geometry=await d.locator('.courier-batch-tools').evaluate(el=>{const x=el.getBoundingClientRect(),a=el.querySelector('h4').getBoundingClientRect(),b=el.querySelector('button').getBoundingClientRect();return {overflow:el.scrollWidth>el.clientWidth,titleRight:a.right,buttonLeft:b.left,width:x.width};});assert.equal(geometry.overflow,false);assert.ok(geometry.buttonLeft>=geometry.titleRight);
   await d.screenshot({path:dir+'/export-button-'+suffix+'.png'});
   await d.locator('[data-next]').click();await p.waitForFunction(()=>document.querySelector('dialog.courier-batch-detail[open] .courier-pager span')?.textContent.includes('51–100'));
   const start=calls.length,all=await exportFile('all-from-page2-'+suffix);assert.deepEqual(calls.slice(start).map(x=>x.offset),[0,100,200,0]);assert.ok(calls.slice(start).every(x=>x.batch_id===fixture.batches[0].id&&!x.keyword&&!x.owner&&!x.history));
   assert.match(await d.locator('.courier-pager span').textContent(),/51–100/);await close();
   const input=view.locator('[name=keyword]');await input.fill('0012345678901');await view.locator('form button:not([type=button])').click();await view.locator('[role=status]').filter({hasText:'匹配 1 批'}).waitFor();await open(0);
   assert.equal(await d.locator('tbody [data-detail]').count(),1);const filtered=await exportFile('all-from-matching-'+suffix);assert.equal(filtered.rows,205);await close();assert.equal(await input.inputValue(),'0012345678901');await view.locator('[data-clear]').click();await view.locator('[data-batch]').first().waitFor();
   await open(1);assert.equal(await d.locator('[data-export]').isDisabled(),true);await close();
   // A partial fetch failure, foreign batch row, and changed snapshots emit no file.
   await open(0);let before=downloads.length;mode={kind:'failure',offset:100};await d.locator('[data-export]').click();await d.locator('[data-export-status]').filter({hasText:'导出失败'}).waitFor();assert.equal(downloads.length,before);assert.equal(await d.locator('[data-export]').isEnabled(),true);await d.screenshot({path:dir+'/export-error-'+suffix+'.png'});mode=null;
   mode={kind:'wrong-batch',offset:100};await d.locator('[data-export]').click();await d.locator('[data-export-status]').filter({hasText:'批次或件数'}).waitFor();assert.equal(downloads.length,before);mode=null;
   mode={kind:'change',offset:100,code:'701000099'+(100+n*2)};await d.locator('[data-export]').click();await d.locator('[data-export-status]').filter({hasText:'明细已变化'}).waitFor();assert.equal(downloads.length,before);
   mode={kind:'change',offset:0,final:true,code:'701000099'+(101+n*2)};await d.locator('[data-export]').click();await d.locator('[data-export-status]').filter({hasText:'明细已变化'}).waitFor();assert.equal(downloads.length,before);mode=null;
   const retried=await exportFile('retry-after-change-'+suffix);assert.equal(retried.rows,205);
   // Double click starts one export; visible progress can be cancelled by close/switch.
   let delivered;const delivery=new Promise(resolve=>delivered=resolve);mode={kind:'hold',offset:0,delivered};const pending=new Promise(resolve=>held=resolve),callStart=calls.length;
   await d.locator('[data-export]').evaluate(el=>{el.click();el.click();});await pending;assert.equal(calls.length,callStart+1);assert.equal(await d.locator('[data-export]').isDisabled(),true);assert.match(await d.locator('[data-export]').textContent(),/导出中/);
   await d.screenshot({path:dir+'/export-progress-'+suffix+'.png'});await close();mode=null;await open(2);before=downloads.length;release();await delivery;await p.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));assert.equal(downloads.length,before);assert.equal(await d.locator('h3').textContent(),fixture.batches[2].batch_no);assert.equal(await d.locator('[data-export-status]').isVisible(),false);await close();
   await view.locator('[data-history]').click();await d.locator('h3').filter({hasText:'无批次'}).waitFor();await p.waitForFunction(()=>!document.querySelector('dialog.courier-batch-detail[open]')?.hasAttribute('aria-busy'));assert.equal(await d.locator('[data-export]').isVisible(),false);await close();
   assert.equal(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);results.push({viewport:width+'x'+height,lang,geometry,all,filtered,retried,downloads:downloads.length,ok:true});await c.close();n++;
  }
  assert.deepEqual(errors,[]);const report={ok:true,pageErrors:errors,results,scenarios:['native toolbar and responsive button','205 records from second page','all batch from single matched waybill','XLSX native text, leading zero and long code','five bilingual fields and Korean midnight','special characters and literal formula-like scanner','batch and history isolation','empty batch disabled','partial failure no file and retry','foreign row rejected','correction between pages rejected','change during final probe rejected','double click one export','close/switch cancels old download','history export hidden']};fs.writeFileSync(dir+'/export-acceptance.json',JSON.stringify(report,null,2));console.log(JSON.stringify(report,null,2));
 }finally{await b.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
