// The existing 002 showLightbox hook also serves inbound/outbound detail thumbnails.
// Real built pages/Worker, loopback and synthetic SQLite/R2 only.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {materialUploadFixture} from './multiple-material-upload-fixture.mjs';
import {png} from './issue-attachment-fixtures.mjs';
const require=createRequire(import.meta.url),{chromium}=require(require.resolve('playwright',{paths:[process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES]}));
const f=await materialUploadFixture(),browser=await chromium.launch({executablePath:'/usr/bin/chromium',headless:true,args:['--no-sandbox']}),errors=[],results=[];
f.DB.raw.prepare("INSERT INTO v2_inbound_plans(id,customer,biz_class,status,created_at,updated_at) VALUES('IB-FE09','Synthetic inbound','bulk','pending','2026-11-04T01:00:00Z','2026-11-04T01:00:00Z')").run();
f.DB.raw.prepare("INSERT INTO v2_outbound_orders(id,customer,biz_class,status,created_at,updated_at) VALUES('OB-FE09','Synthetic outbound','bulk','pending_issue','2026-11-04T01:00:00Z','2026-11-04T01:00:00Z')").run();
for(const [id,type,document,category]of [['inbound','inbound_plan','IB-FE09','other'],['vehicle','outbound_order','OB-FE09','vehicle_photo'],['outbound','outbound_order','OB-FE09','other']]){
 const key='synthetic-preview/'+id+'.png';f.DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key,file_name,file_size,content_type) VALUES(?,?,?,?,?,?,?,?)').run(id,type,document,category,key,id+'.png',png.length,'image/png');f.objects.set(key,{body:png,httpMetadata:{contentType:'image/png'}});
}
const original=f.DB.raw.prepare('SELECT * FROM v2_attachments ORDER BY id').all();
try{for(const width of [1440,390]){
 const context=await browser.newContext({viewport:{width,height:900}}),page=await context.newPage();page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.accept());
 await context.addCookies([{name:'ck_office_access',value:f.cookie.split('=')[1],url:f.origin}]);await context.addInitScript(()=>{window.owned={created:[],revoked:[]};const create=URL.createObjectURL.bind(URL),revoke=URL.revokeObjectURL.bind(URL);URL.createObjectURL=b=>{const u=create(b);owned.created.push(u);return u;};URL.revokeObjectURL=u=>{owned.revoked.push(u);revoke(u);};});await page.route('**/*',r=>new URL(r.request().url()).origin===f.origin?r.continue():r.abort());await page.goto(f.origin+'/002/');await page.waitForFunction(()=>window.CKSession?.user&&window.CKIssueImages);
 for(const [view,container,document,types]of [['inbound','#inboundDetailBody','IB-FE09',['inbound']],['outbound','#outboundDetailBody','OB-FE09',['vehicle','outbound']]]){
  await page.evaluate(({view,document})=>view==='inbound'?openInboundDetail(document):openOutboundDetail(document),{view,document});
  for(const type of types){const thumb=page.locator(container+' img[onclick*="showLightbox"]').filter({visible:true});const chosen=page.locator(container+' img[onclick*="showLightbox"][src*="'+type+'.png"]');await chosen.waitFor();await chosen.click();await page.waitForFunction(()=>document.querySelector('#lightboxImg')?.naturalWidth===240&&document.querySelector('[data-view-status]')?.textContent.includes('已加载'));assert.equal(await page.locator('#lightbox').isVisible(),true);assert.match(await page.locator('[data-view-download]').getAttribute('href'),new RegExp(type+'\\.png'));await page.locator('[data-view-close]').click();await page.waitForFunction(()=>owned.created.every(u=>owned.revoked.includes(u)));results.push({width,case:view+'-'+type,passed:true});}
 }
 await context.close();console.log('PASS actual showLightbox inbound/outbound vehicle/general '+width);
 }assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_attachments ORDER BY id').all(),original);for(const object of f.objects.values())assert.deepEqual(object.body,png);assert.deepEqual(errors,[]);assert.equal(results.length,6);console.log(JSON.stringify({results,errors,attachmentsUnchanged:true}));
}finally{await browser.close();await f.close();}
