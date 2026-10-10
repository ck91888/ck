// Copy into tests/ and run node --test tests/workflow-version.candidate.test.mjs.
// CK_REVIEW_REPO optionally selects a checkout. Every write is in-memory only.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const repo=process.env.CK_REVIEW_REPO|| (fs.existsSync(path.join(here,'d1-adapter.mjs'))?path.resolve(here,'..'):path.resolve(here,'../../ck-system-review-03a32e9'));
const {default:worker}=await import(pathToFileURL(path.join(repo,'worker-v2/index.js')));
const {database}=await import(pathToFileURL(path.join(repo,'tests/d1-adapter.mjs')));
const source=file=>fs.readFileSync(path.join(repo,file),'utf8');
function fixture(t){
 const DB=database(),objects=new Map(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true',SOP_WORK_CHAIN_ENABLED:'true',R2_BUCKET:{async put(key,body,meta){objects.set(key,{body:await new Response(body).arrayBuffer(),...meta});},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}}};
 t.after(()=>DB.raw.close());
 const raw=async body=>(await worker.fetch(new Request('https://fixture.invalid/api',{method:'POST',headers:body instanceof FormData?{}:{'Content-Type':'application/json'},body:body instanceof FormData?body:JSON.stringify(body)}),env)).json();
 const call=(action,data={})=>raw({action,client_req_id:crypto.randomUUID(),...data});
 const ok=async(action,data={})=>{const r=await call(action,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const get=async id=>(await ok('sop_get',{id})).record;
 const change=async(action,id,data={})=>call(action,{id,revision:(await get(id)).revision,...data});
 const changeOk=async(action,id,data={})=>{const r=await change(action,id,data);assert.equal(r.ok,true,action+': '+(r.error||r.message));return r;};
 const start=(action,data={},person='A')=>ok('sop_native_start',{payload:{action,client_req_id:crypto.randomUUID(),...data},workers:[{id:'QA-'+person,name:'QA '+person}],lead_id:'QA-'+person,estimated_minutes:10,labor_department:'bulk'});
 return{DB,env,objects,raw,call,ok,get,change,changeOk,start};
}
async function respondedIssue(f){
 const issue=await f.ok('v2_issue_create',{biz_class:'bulk',customer:'QA version review',issue_description:'QA inspect label'});
 const first=await f.start('v2_issue_handle_start',{issue_id:issue.id});
 const adopted=await f.ok('sop_issue_adopt',{legacy_id:issue.id,native:true});
 await f.changeOk('sop_issue_feedback',adopted.id,{message:'QA original requirements completed'});
 return{issue,adopted,first};
}
function printHarness(){
 const documents=[],codes=[],qrcode=()=>({addData(value){codes.push(value);},make(){},createSvgTag(){return '<svg></svg>';}});
 qrcode.stringToBytesFuncs={'UTF-8':()=>{}};
 const window={open(){let html='';return{document:{write(x){html+=x;},close(){documents.push(html);}},focus(){},print(){}};}};
 const context={window,qrcode};vm.createContext(context);vm.runInContext(source('shared/document-labels.js'),context);
 return{window,documents,codes};
}
function formHarness(record,fields,api){
 const elements=new Map(),node=id=>{if(!elements.has(id))elements.set(id,{id,textContent:'',innerHTML:'',disabled:false,value:'',dataset:{},classList:{toggle(){}},showModal(){this.open=true;},elements:[{name:'pallet'},{name:'barcode'}],querySelectorAll(){return[];}});return elements.get(id);};
 class FixtureFormData{constructor(){this.values={...fields};}*[Symbol.iterator](){yield* Object.entries(this.values);}}
 const context={current:record,cargoDraft:null,planObserver:null,copy:null,$:node,crypto,FormData:FixtureFormData,TypeError,notice(){},closeModal:async()=>{},api,detail:async()=>{},load:async()=>{}};
 vm.createContext(context);const text=source('shared/sop-native.js');
 vm.runInContext(text.slice(text.indexOf('function form(title'),text.indexOf("$('cancel').onclick=closeModal;")),context);
 context.form('QA scan','QA fields','sop_check_scan',v=>v,async()=>{});
 return{fields,node,context,submit:()=>node('editor').onsubmit({preventDefault(){}})};
}
async function check(f,date='2099-10-12'){
 const c=await f.ok('sop_check_create',{ship_date:date});
 await f.changeOk('sop_check_items',c.id,{items:[{barcode:'QA-BARCODE-A',customer:'QA customer',qty:2},{barcode:'QA-BARCODE-B',customer:'QA customer',qty:1}]});
 return f.get(c.id);
}
test('cancelled source plan cannot print its work requirements through the real group renderer',async t=>{
 const f=fixture(t),p=await f.ok('v2_inbound_plan_create',{customer:'QA print cancel',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:20}],work_requests:[{title:'QA one',instructions:'QA first instruction',department:'bulk',planned_quantity:10,planned_unit:'box'},{title:'QA two',instructions:'QA second instruction',department:'bulk',planned_quantity:10,planned_unit:'box'}]});
 await f.ok('v2_inbound_plan_cancel',{inbound_plan_id:p.id,operator_name:'QA manager',reason:'QA synthetic cancellation'});
 const res=await f.ok('sop_need_groups',{source_type:'inbound',source_id:p.id}),h=printHarness();
 for(const group of res.items||res.groups||[]){try{await h.window.CKNeedBatchPrint(group);}catch(e){assert.match(e.message,/cancel|取消|可打印|인쇄|취소/i);}}
 assert.equal(h.documents.length,0,'normal plan cancellation must prevent printing cancelled-source sheets');
});
test('closed native issue rejects feedback without reopening or regressing its legacy status',async t=>{
 const f=fixture(t),x=await respondedIssue(f);await f.changeOk('sop_issue_close',x.adopted.id);
 const before=await f.get(x.adopted.id),feedback=await f.change('sop_issue_feedback',x.adopted.id,{message:'QA accidental feedback after office close'});
 assert.equal(feedback.ok,false);const after=await f.get(x.adopted.id);
 assert.equal(after.status,'closed');assert.equal(after.messages.length,before.messages.length);
 assert.equal(f.DB.raw.prepare('SELECT status FROM v2_issue_tickets WHERE id=?').get(x.issue.id).status,'completed');
});
test('native issue close cannot leave a second active processing run and open labor segment behind',async t=>{
 const f=fixture(t),x=await respondedIssue(f),second=await f.start('v2_issue_handle_start',{issue_id:x.issue.id},'B');
 const close=await f.change('sop_issue_close',x.adopted.id);
 assert.equal(close.ok,false,'an active processing round requires warehouse feedback before office close');
 assert.notEqual((await f.get(x.adopted.id)).status,'closed');
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(second.job_id).n,1);
});
test('native issue close transaction rejects a processing round created after its preflight read',async t=>{
 const f=fixture(t),x=await respondedIssue(f),batch=f.DB.batch.bind(f.DB);let second=null,interleaved=false;
 f.DB.batch=async statements=>{
  if(!interleaved&&statements.some(s=>s.sql.includes('INSERT INTO sop_events')&&s.args?.[3]==='sop_issue_close')){
   interleaved=true;f.DB.batch=batch;
   second=await f.start('v2_issue_handle_start',{issue_id:x.issue.id},'B');
  }
  return batch(statements);
 };
 const close=await f.change('sop_issue_close',x.adopted.id);
 assert.equal(interleaved,true,'a real second start must commit between preflight and final close batch');
 assert.equal(close.ok,false,'transaction must recheck the source run and open labor gate');
 assert.equal((await f.get(x.adopted.id)).status,'responded');
 assert.equal(f.DB.raw.prepare('SELECT status FROM v2_issue_tickets WHERE id=?').get(x.issue.id).status,'processing');
 assert.equal(f.DB.raw.prepare('SELECT run_status FROM v2_issue_handle_runs WHERE job_id=?').get(second.job_id).run_status,'working');
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(second.job_id).n,1);
});
test('actual scan form edit after transport failure builds a new input intent',async t=>{
 const f=fixture(t),record=await check(f),fields={pallet:'QA-PALLET',barcode:'QA-BARCODE-A'},sent=[];let fail=true;
 const h=formHarness(record,fields,async(action,body)=>{sent.push(structuredClone(body));if(fail){fail=false;throw new TypeError('QA pre-commit transport failure');}return f.ok(action,body);});
 await h.submit();fields.barcode='QA-BARCODE-B';if(h.node('editor').oninput)h.node('editor').oninput({target:{name:'barcode',value:fields.barcode}});await h.submit();
 assert.equal(sent.length,2);assert.equal(sent[1].barcode,'QA-BARCODE-B');assert.notEqual(sent[1].client_req_id,sent[0].client_req_id);
 const after=await f.get(record.id);assert.equal(after.items.find(x=>x.barcode==='QA-BARCODE-A').scanned,0);assert.equal(after.items.find(x=>x.barcode==='QA-BARCODE-B').scanned,1);
});
for(const committed of [false,true])test('same-value actual scan form transport retry preserves request ID and counts once (commit='+committed+')',async t=>{
 const f=fixture(t),record=await check(f),fields={pallet:'QA-PALLET',barcode:'QA-BARCODE-A'},sent=[];let fail=true;
 const h=formHarness(record,fields,async(action,body)=>{sent.push(structuredClone(body));if(fail){fail=false;if(committed)await f.ok(action,body);throw new TypeError('QA transport failure');}return f.ok(action,body);});
 await h.submit();await h.submit();assert.equal(sent.length,2);assert.equal(sent[0].client_req_id,sent[1].client_req_id);
 assert.equal((await f.get(record.id)).items.find(x=>x.barcode==='QA-BARCODE-A').scanned,1);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM sop_events WHERE request_id=?').get(sent[0].client_req_id).n,1);
});
test('closed issue explicit requirement change reopens with exact version acknowledgement and retry-safe feedback',async t=>{
 const f=fixture(t),x=await respondedIssue(f);await f.changeOk('sop_issue_close',x.adopted.id);const old=await f.get(x.adopted.id);
 await f.changeOk('sop_issue_append',x.adopted.id,{message:'QA explicit second requirement'});const current=await f.get(x.adopted.id);
 assert.equal(current.status,'open');assert.equal(current.requirement_version,old.requirement_version+1);
 assert.equal((await f.change('sop_issue_ack',current.id,{requirement_version:old.requirement_version})).ok,false);
 assert.equal((await f.change('sop_issue_feedback',current.id,{message:'QA premature feedback'})).ok,false);
 await f.changeOk('sop_issue_ack',current.id,{requirement_version:current.requirement_version});
 const before=await f.get(current.id),payload={id:current.id,revision:before.revision,client_req_id:'qa-issue-feedback-once',message:'QA new requirement completed'};
 const accepted=await f.ok('sop_issue_feedback',payload);assert.deepEqual(await f.ok('sop_issue_feedback',payload),accepted);
 assert.equal((await f.get(current.id)).messages.filter(m=>m.feedback).length,2);await f.changeOk('sop_issue_close',current.id);
 assert.equal((await f.get(current.id)).status,'closed');
});
test('concurrent scan revisions reject stale intent and exact committed replay adds no scan',async t=>{
 const f=fixture(t),record=await check(f),payload={id:record.id,revision:record.revision,pallet:'QA-PALLET',barcode:'QA-BARCODE-A',client_req_id:'qa-scan-revision-once'};
 const accepted=await f.ok('sop_check_scan',payload);assert.deepEqual(await f.ok('sop_check_scan',payload),accepted);
 assert.equal((await f.call('sop_check_scan',{...payload,barcode:'QA-BARCODE-B',client_req_id:'qa-stale-different-scan'})).ok,false);
 const after=await f.get(record.id);assert.equal(after.items.find(x=>x.barcode==='QA-BARCODE-A').scanned,1);assert.equal(after.items.find(x=>x.barcode==='QA-BARCODE-B').scanned,0);
});
test('returned task preserves separate round clocks and records only one accepted result on retry',async t=>{
 const f=fixture(t),task=await f.ok('sop_task_create',{department:'bulk',title:'QA rework',job_type:'bulk_op',workers:[{id:'QA-A',name:'QA A'}],lead_id:'QA-A',estimated_minutes:10});
 await f.changeOk('sop_task_start',task.id);await f.changeOk('sop_task_finish',task.id,{result:{quantity:10,unit:'box',description:'QA round one'}});
 await f.changeOk('sop_task_review',task.id,{decision:'return',reason:'QA label correction'});
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(task.id).n,0);
 const rework=await f.get(task.id),start={id:task.id,revision:rework.revision,client_req_id:'qa-rework-start-once'};
 await f.ok('sop_task_start',start);await f.ok('sop_task_start',start);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=?').get(task.id).n,2);
 await f.changeOk('sop_task_finish',task.id,{result:{quantity:8,unit:'box',description:'QA corrected result'}});
 const pending=await f.get(task.id),pass={id:task.id,revision:pending.revision,client_req_id:'qa-rework-pass-once',decision:'pass',reason:'QA accepted'};
 await f.ok('sop_task_review',pass);await f.ok('sop_task_review',pass);
 assert.equal(f.DB.raw.prepare('SELECT count(*) n FROM v2_ops_job_results WHERE job_id=?').get(task.id).n,1);
 assert.equal(JSON.parse(f.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(task.id).result_json).quantity,8);
 assert.equal(f.DB.raw.prepare("SELECT count(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(task.id).n,0);
});
test('actual verify list late reply preserves the new-check workflow mounted by the real new button callback and mount',async()=>{
 let resolveList;const nodes=new Map(),node=id=>{if(!nodes.has(id))nodes.set(id,{id,innerHTML:'',value:'',dataset:{},querySelector(){return null;}});return nodes.get(id);};
 // Mock rendering only: ownership/invalidation must come from the actual mount,
 // otherwise a production ownership fix cannot be exercised by this fixture.
 const context={document:{getElementById:node},api:()=>new Promise(resolve=>{resolveList=resolve;}),getPager:()=>({limit:20}),getOffset:()=>0,renderPager:()=>'',esc:String,verifyStatusLabel:String,_currentView:'check',_currentTab:'check',goView(name){context._currentView=name;},CKWorkflow(body){body.innerHTML='<section data-ck-workflow="true">QA CHECK new workflow</section>';return{destroy(){}};}};
 vm.createContext(context);const app=source('002/app.js');vm.runInContext(app.slice(app.indexOf('async function loadVerifyList()'),app.indexOf('function verifyStatusLabel(s)')),context);
 const entry=source('shared/sop-entry.js'),start=entry.indexOf("document.getElementById('btnNewCheck').onclick=");assert.ok(start>=0);
 const stateStart=entry.indexOf('let instance=null,activeRoot=null;'),mountStart=entry.indexOf('function mount(root,options)'),mountEnd=entry.indexOf('function button',mountStart);
 assert.ok(stateStart>=0&&mountStart>=0&&mountEnd>mountStart);
 vm.runInContext(entry.slice(stateStart,entry.indexOf('const detailNeeds=',stateStart))+entry.slice(mountStart,mountEnd),context);
 vm.runInContext(entry.slice(start,entry.indexOf('\n',start)),context);
 const pending=context.loadVerifyList();node('btnNewCheck').onclick();assert.match(node('checkListBody').innerHTML,/data-ck-workflow/);
 resolveList({ok:true,items:[],total:0});await pending;
 assert.match(node('checkListBody').innerHTML,/data-ck-workflow/,'late original list response cannot overwrite a newer mounted workflow');
});
test('file upload replay increments versions once; stale upload and cross-need files are rejected; old QR resolves latest',async t=>{
 const f=fixture(t),make=()=>f.ok('sop_need_create',{title:'QA files',customer:'QA files',source_type:'inventory',supply_chain_no:'QA-STOCK',instructions:'QA ten cartons',planned_quantity:10,planned_unit:'box',department:'bulk',owner:'QA office'});
 const a=await make(),b=await make(),before=await f.get(a.id),form=new FormData();
 for(const [k,v]of Object.entries({action:'v2_attachment_upload',related_doc_type:'sop_need',related_doc_id:a.id,attachment_category:'work_material',material_kind:'pallet_label',revision:before.revision,client_req_id:'qa-file-upload-once'}))form.set(k,v);
 form.set('file',new File(['%PDF-1.7\nQA fixture label'],'qa-label.pdf',{type:'application/pdf'}));
 const upload=await f.raw(form);assert.equal(upload.ok,true,upload.error);assert.equal((await f.raw(form)).id,upload.id);
 const after=await f.get(a.id);assert.equal(after.material_version,(before.material_version||0)+1);assert.equal(after.requirement_version,(before.requirement_version||1)+1);assert.equal(f.objects.size,1);
 form.set('client_req_id','qa-stale-file-new-intent');assert.equal((await f.raw(form)).ok,false);
 assert.equal((await f.ok('sop_work_materials',{id:b.id})).items.length,0);
 const old=await f.ok('sop_field_resolve',{code:'CKWORK|'+a.id+'|'+(before.requirement_version||1)});assert.equal(old.changed,true);assert.equal(old.need.requirement_version,after.requirement_version);
 const legacy=await f.ok('sop_field_resolve',{code:a.id});assert.equal(legacy.changed,false);assert.equal(legacy.need.id,a.id);
 const h=printHarness();await h.window.CKNeedPrint(after);assert.deepEqual(h.codes,['CKWORK|'+a.id+'|'+after.requirement_version]);assert.equal(h.documents.length,1);
});
