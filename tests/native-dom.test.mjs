// DOM integration tests, not a substitute for real browser/device visual acceptance.
// Run after build with CK_JSDOM_MODULE pointing at an installed jsdom API module.
import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
import * as documentCode from '../shared/document-code.js';
import * as courierRules from '../shared/courier-rules.js';
import * as laborDepartment from '../shared/labor-department.js';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null;
const assets=path.resolve('worker-v2/.sop-staging-assets');
async function fixture(){
 const env={DB:database(),SOP_ENVIRONMENT:'staging',SOP_ATTENDANCE_ENABLED:'true',SOP_UPGRADE_ENABLED:'true',SOP_AUTO_OUTBOUND:'true',SOP_ROLLOUT_DEPARTMENTS:'bulk',SOP_USERS_JSON:JSON.stringify([{id:'M',name:'测试负责人',role:'manager',key:crypto.randomUUID()}])};
 let cookie='';async function request(body){return worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie},body:JSON.stringify(body)}),env);}
 const login=await request({action:'sop_login',sop_key:JSON.parse(env.SOP_USERS_JSON)[0].key});cookie=login.headers.get('set-cookie').split(';')[0];
 const seed=await (await request({action:'sop_demo_prepare',client_req_id:'dom-seed'})).json();assert.equal(seed.ok,true,seed.error);
 async function page(url,storage={}){
  const errors=[];const {JSDOM,requestInterceptor,VirtualConsole}=runtime;const vc=new VirtualConsole();vc.on('jsdomError',e=>{if(!/Not implemented:|Could not parse CSS stylesheet/.test(e.message))errors.push(e.message)});
  // jsdom does not execute dynamic module imports; inject the actual module exports below.
  const resources={interceptors:[requestInterceptor(req=>{const u=new URL(req.url);if(u.origin!=='https://fixture.test')throw Error('External request rejected: '+u.origin);let source=fs.readFileSync(path.join(assets,u.pathname),'utf8');source=source.replace(/import\('\/shared\/([a-z-]+)\.js'\)/g,(_m,name)=>'Promise.resolve(window.__fixtureModules["'+name+'"])');return new Response(source,{headers:{'Content-Type':u.pathname.endsWith('.js')?'application/javascript':'text/css'}});})]};
  const u=new URL(url,'https://fixture.test');const html=fs.readFileSync(path.join(assets,u.pathname,'index.html'),'utf8');
  const dom=new JSDOM(html,{url:u.href,runScripts:'dangerously',resources,virtualConsole:vc,pretendToBeVisual:true,beforeParse(w){
   for(const [k,v] of Object.entries(storage))w.sessionStorage.setItem(k,v);
   w.CKDocumentCode=documentCode;
   w.__fixtureModules={'document-code':documentCode,'courier-rules':courierRules,'labor-department':laborDepartment};
   w.fetch=async(url,options={})=>{const endpoint=new URL(url,w.location.href);assert.equal(endpoint.origin,'https://fixture.test');assert.ok(['/api','/001/api','/attendance/api'].includes(endpoint.pathname));return request(JSON.parse(options.body));};
   w.alert=message=>errors.push('alert: '+message);w.confirm=()=>true;w.prompt=()=>{throw Error('Unexpected login/name prompt')};w.scrollTo=()=>{};w.matchMedia=()=>({matches:false,addEventListener(){},removeEventListener(){}});
   w.HTMLDialogElement.prototype.showModal=function(){this.open=true};w.HTMLDialogElement.prototype.close=function(){this.open=false};
  }});
  await until(()=>dom.window.CKSession?.user,errors);await until(()=>dom.window.document.querySelector('.ck-cross-nav'),errors);
  await new Promise(resolve=>setTimeout(resolve,40));assert.ok(!dom.window.document.body.textContent.includes('页面初始化失败'),dom.window.document.querySelector('.ck-updates')?.textContent);
  return {dom,w:dom.window,d:dom.window.document,errors};
 }
 return {page,seed,request,env};
}
async function until(condition,errors=[]){for(let i=0;i<100;i++){if(condition())return;await new Promise(r=>setTimeout(r,10));}throw Error('DOM condition timeout: '+errors.join('; '));}
const opts={skip:!runtime};
test('shared batch file controls confirm whole-plan removal, preserve retry identity and restore localized rows',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';
 const call=async(action,b={})=>{const r=await(await f.request({action,client_req_id:crypto.randomUUID(),...b})).json();assert.equal(r.ok,true,r.error);return r;};
 const plan=await call('v2_inbound_plan_create',{customer:'QA DOM shared',biz_classes:['bulk']}),n=await call('sop_need_create',{customer:'QA DOM shared',title:'QA shared work',instructions:'QA instructions',source_type:'inbound',source_id:plan.id,department:'bulk',owner:'QA owner',planned_quantity:10,planned_unit:'箱'});
 f.env.DB.raw.prepare("INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,created_at) VALUES('QA-DOM-BATCH','inbound_plan',?,'batch_work_material','QA-shared.csv','qa-blob',?)").run(plan.id,new Date().toISOString());
 const p=await f.page('/002/');try{
  const host=p.d.createElement('section');p.d.body.append(host);const group={source_type:'inbound',source_id:plan.id,items:[{id:n.id}]};await p.w.CKWorkChain.batchMaterials(host,group);
  const alerts=[];p.w.alert=m=>alerts.push(m);p.w.confirm=()=>false;host.querySelector('[data-batch-material-action=remove]').click();await new Promise(r=>setTimeout(r,15));assert.equal(f.env.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,0);
  let confirm='';p.w.confirm=m=>{confirm=m;return true;};const original=p.w.CKSession.request,attempts=[];let lost=false;
  p.w.CKSession.request=async(action,body)=>{const r=await original(action,body);if(action==='sop_batch_work_material_remove'){attempts.push(JSON.stringify(body));if(!lost){lost=true;throw new TypeError('QA response lost');}}return r;};
  const remove=host.querySelector('[data-batch-material-action=remove]');remove.click();await until(()=>alerts.length,p.errors);assert.match(confirm,/全部关联作业/);assert.match(confirm,/可恢复/);assert.equal(remove.disabled,false);remove.click();await until(()=>host.querySelector('[data-batch-material-action=restore]'),p.errors);assert.equal(attempts[0],attempts[1]);assert.equal(f.env.DB.raw.prepare('SELECT count(*) n FROM ck_batch_material_events').get().n,1);
  p.w.setLang('ko');p.w.applyLang();assert.equal(host.querySelector('[data-batch-material-action=restore]').textContent,'복구');host.querySelector('[data-batch-material-action=restore]').click();await until(()=>host.querySelector('[data-batch-material-action=remove]'),p.errors);assert.equal(host.querySelector('[data-batch-material-action=remove]').textContent,'제거');assert.equal(host.querySelector('.chain-removed-materials'),null);
  p.w.CKSession.user.scope='field';await p.w.CKWorkChain.batchMaterials(host,group);assert.equal(host.children.length,0);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
async function issueFixture(){const f=await fixture(),call=async(action,data={})=>{const r=await(await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();assert.equal(r.ok,true,r.error);return r;},issue=await call('v2_issue_create',{biz_class:'bulk',customer:'QA DOM issue',issue_description:'QA original issue requirement'});const get=async()=> (await call('sop_get',{id:'ISSUE-'+issue.id})).record;return{...f,call,issue,get};}
test('issue workflow refresh updates the original field requirement and closed state, not only the added panel',opts,async()=>{
 const f=await issueFixture(),p=await f.page('/001/');try{p.w.openIssue(f.issue.id);await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #actions'),p.errors);let n=await f.get();await f.call('sop_issue_change',{id:n.id,revision:n.revision,message:'QA updated green label'});
  const refresh=()=>p.d.querySelector('#issueDetailBody .ck-workflow #refresh').click();refresh();await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #detailBody')?.textContent.includes('QA updated green label'),p.errors);
  assert.ok([...p.d.querySelectorAll('#issueDetailBody .detail-section')].some(x=>x.textContent.includes('QA updated green label')),'original requirement section must refresh with latest workflow requirements');
  n=await f.get();await f.call('sop_issue_ack',{id:n.id,revision:n.revision,requirement_version:n.requirement_version});n=await f.get();await f.call('sop_issue_feedback',{id:n.id,revision:n.revision,message:'QA finished'});n=await f.get();await f.call('sop_issue_close',{id:n.id,revision:n.revision});refresh();await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow .status')?.textContent==='已关闭',p.errors);
  assert.match(p.d.querySelector('#issueDetailBody .detail-field .st')?.textContent||'',/完成/);assert.equal(p.d.querySelector('#issueDetailBody button[onclick*="handleIssueStart"]'),null);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('saving issue requirements from original office workflow refreshes retained original details',opts,async()=>{
 const f=await issueFixture(),p=await f.page('/002/?issue='+f.issue.id);try{await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #actions'),p.errors);[...p.d.querySelectorAll('#issueDetailBody .ck-workflow #actions button')].find(x=>x.textContent==='修改作业要求').click();await until(()=>p.d.querySelector('dialog[open]'),p.errors);const form=p.d.querySelector('dialog[open] #editor');form.elements.message.value='QA saved updated office instruction';form.requestSubmit();await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #detailBody')?.textContent.includes('QA saved updated office instruction')&&!p.d.querySelector('dialog[open]'),p.errors);
  assert.ok([...p.d.querySelectorAll('#issueDetailBody .detail-section')].some(x=>x.textContent.includes('QA saved updated office instruction')),'original office description must update after save');assert.equal((await f.get()).changes.length,1);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('issue workflow buttons follow the same existing roles as backend without exposing forbidden field mutations',opts,async()=>{
 const f=await issueFixture(),p=await f.page('/001/');try{p.w.openIssue(f.issue.id);await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #actions'),p.errors);let n=await f.get();await f.call('sop_issue_feedback',{id:n.id,revision:n.revision,message:'QA responded'});
  for(const role of ['manager','service','dispatcher','reviewer']){p.w.CKSession.user.role=role;await p.w.loadIssueDetail();await until(()=>p.d.querySelector('#issueDetailBody .ck-workflow #actions'),p.errors);const labels=[...p.d.querySelectorAll('#issueDetailBody .ck-workflow #actions button')].map(x=>x.textContent);assert.equal(labels.includes('修改作业要求'),['manager','service'].includes(role),role);assert.equal(labels.includes('追加说明'),['manager','service'].includes(role),role);assert.equal(labels.includes('确认关闭'),['manager','service'].includes(role),role);assert.equal(labels.includes('仓库反馈'),['manager','dispatcher','reviewer'].includes(role),role);}
  assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inbound work-plan add button keeps its action and caption through language changes',opts,async()=>{
 const f=await fixture(),p=await f.page('/002/?tab=inbound');
 try{
  p.w.goTab('inbound_create');await until(()=>p.d.querySelector('[data-add-work]'),p.errors);
  const button=p.d.querySelector('[data-add-work]'),list=button.parentElement.querySelector('[data-work-list]');
  p.w.applyLang();assert.equal(button.textContent,'新增');
  button.click();assert.equal(list.children.length,1);button.click();assert.equal(list.children.length,2);
  const remove=list.firstElementChild.querySelector('[data-remove]');assert.match(remove.textContent,/删除此计划/);
  p.w.setLang('ko');p.w.applyLang();assert.equal(button.textContent,'추가');
  assert.match(remove.textContent,/삭제/);button.click();assert.equal(list.children.length,3);
  p.w.setLang('zh');p.w.applyLang();assert.equal(button.textContent,'新增');
  remove.click();assert.equal(list.children.length,2);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inbound deferred references survive lost responses and retain a visible backfill audit',opts,async()=>{
 const f=await fixture(),created=await(await f.request({action:'v2_inbound_plan_create',customer:'QA-DOM deferred',biz_classes:['direct_ship','bulk_putaway'],client_req_id:crypto.randomUUID()})).json();assert.equal(created.ok,true,created.error);
 const p=await f.page('/002/?tab=inbound');
 try{p.w.openInboundDetail(created.id);await until(()=>p.d.querySelector('.ck-inbound-reference>button'),p.errors);
  const button=p.d.querySelector('.ck-inbound-reference>button');button.click();const form=p.d.querySelector('.ck-inbound-reference form');
  assert.ok([...form.querySelectorAll('textarea')].every(x=>!x.required));form.querySelector('[data-reference=direct_ship]').value='QA-DOM-D1\nQA-DOM-D2';form.querySelector('[data-reference=bulk_putaway]').value='QA-DOM-B1';
  const original=p.w.CKSession.request;let lost=false;const attempts=[];
  p.w.CKSession.request=async(action,body)=>{const result=await original(action,body);if(action==='v2_inbound_plan_bind_external'){attempts.push(JSON.stringify(body));if(!lost){lost=true;throw Error('QA simulated lost response');}}return result;};
  form.requestSubmit();await until(()=>form.querySelector('[role=alert]').textContent.includes('lost response'),p.errors);assert.equal(form.querySelector('[data-reference=direct_ship]').value,'QA-DOM-D1\nQA-DOM-D2');
  form.requestSubmit();await until(()=>p.d.querySelectorAll('.ck-inbound-reference .ck-reference-supplemented').length===2,p.errors);assert.equal(attempts[0],attempts[1]);assert.equal(p.d.querySelector('.ck-inbound-reference .ck-reference-pending'),null);
  await p.w.loadInboundDetail();assert.equal(p.d.querySelectorAll('.ck-reference-supplemented').length,2);assert.ok(p.d.querySelector('.ck-reference-supplemented').textContent.includes('外部入库单号已补充'));
  assert.equal(f.env.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_reference_history WHERE plan_id=?').get(created.id).n,2);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inbound print preview has no issue side effect and explicit confirmation marks only the printed version',opts,async()=>{
 const f=await fixture(),created=await(await f.request({action:'v2_inbound_plan_create',customer:'QA-DOM issue',biz_classes:['bulk'],client_req_id:crypto.randomUUID()})).json();assert.equal(created.ok,true,created.error);
 const p=await f.page('/002/?tab=inbound');
 try{p.w.openInboundDetail(created.id);await until(()=>p.d.querySelector('.ck-inbound-issue'),p.errors);let button=p.d.querySelector('.ck-inbound-issue button');assert.equal(button.disabled,true);
  let printHtml='';p.w.open=()=>({document:{open(){printHtml='';},write(x){printHtml+=x;},close(){}},close(){}});
  await p.w.printIbQr();assert.match(printHtml,/入库计划单/);assert.match(printHtml,/计划版本/);assert.equal(f.env.DB.raw.prepare('SELECT count(*) n FROM ck_inbound_document_issues').get().n,0);
  button=p.d.querySelector('.ck-inbound-issue button');assert.equal(button.disabled,false);button.click();await until(()=>p.d.querySelector('.ck-inbound-issue .ck-inbound-issue-status')?.textContent.includes('已打印下发'),p.errors);
  assert.equal(f.env.DB.raw.prepare('SELECT status FROM v2_inbound_plans WHERE id=?').get(created.id).status,'pending');
  await(await f.request({action:'v2_inbound_plan_update',id:created.id,biz_classes:['bulk'],remark:'QA changed cargo instruction',client_req_id:crypto.randomUUID()})).json();await p.w.loadInboundDetail();assert.match(p.d.querySelector('.ck-inbound-issue .ck-inbound-issue-status').textContent,/重新打印/);assert.equal(p.d.querySelector('.ck-inbound-issue button').disabled,true);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('work completion-date editor, detail and print share the Korean day while history remains readable',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';const created=await(await f.request({action:'sop_need_create',source_type:'inventory',department:'bulk',customer:'QA-date DOM',title:'QA-date DOM work',supply_chain_no:'QA-date stock',instructions:'QA-date original requirements',deadline:'2026-10-05',client_req_id:crypto.randomUUID()})).json();assert.equal(created.ok,true,created.error);
 const row=f.env.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(created.id),legacy=JSON.parse(row.state);legacy.deadline='2026-10-04T15:00:01Z';f.env.DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(legacy),created.id);
 const p=await f.page('/002/?need='+created.id+'&individual=1');
 try{await until(()=>p.d.querySelector('.ck-work-audit'),p.errors);const content=p.d.querySelector('#view-need #content');assert.match(content.querySelector('article').textContent,/要求完成日期：2026-10-05/);assert.ok(!content.querySelector('article').textContent.includes('15:00:01'));
  [...content.querySelectorAll('#actions button')].find(b=>b.textContent==='修改作业要求').click();await until(()=>p.d.querySelector('#view-need dialog[open]'),p.errors);const form=p.d.querySelector('#view-need #editor');assert.equal(form.elements.deadline.type,'date');assert.equal(form.elements.deadline.value,'2026-10-05');assert.match(form.elements.deadline.closest('label').textContent,/要求完成日期/);form.elements.instructions.value='QA-date revised requirements';assert.ok(form.checkValidity(),[...form.elements].filter(e=>!e.checkValidity()).map(e=>e.name+': '+e.validationMessage).join('; '));form.requestSubmit();await until(()=>!p.d.querySelector('#view-need dialog[open]')||p.d.querySelector('#view-need #formError').textContent,p.errors);assert.equal(p.d.querySelector('#view-need #formError').textContent,'');await until(()=>content.querySelector('article').textContent.includes('revised requirements'),p.errors);
  await until(()=>content.querySelector('.ck-work-audit'),p.errors);assert.equal(JSON.parse(f.env.DB.raw.prepare('SELECT state FROM sop_records WHERE id=?').get(created.id).state).deadline,legacy.deadline);assert.match(content.querySelector('.ck-work-audit').textContent,/修改作业要求/);assert.ok(!content.querySelector('.ck-work-audit').textContent.includes('sop_need_update'));
  let printed='';p.w.open=()=>({document:{write(h){printed=h;},close(){}},focus(){},print(){}});await p.w.CKNeedPrint({id:created.id});assert.match(printed,/要求完成日期：2026-10-05/);assert.ok(!printed.includes('15:00:01'));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inventory-plan editor keeps unbooked work independent, defers crew assignment and clears cancelled drafts',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';const p=await f.page('/002/?tab=need');
 try{await until(()=>p.d.querySelector('.ck-needs-tools'),p.errors);p.d.querySelector('.ck-needs-tools button:nth-child(2)').click();await until(()=>p.d.querySelector('dialog[open] [name=title]'),p.errors);const editor=p.d.querySelector('#editor');
  assert.equal(editor.querySelector('[name=owner],[name=status],[name=source_id],[name=reason]'),null);assert.equal(editor.elements.source_type.value,'inventory');assert.equal(editor.elements.supply_chain_no.required,true);assert.equal(editor.elements.planned_unit.value,'');assert.equal(editor.elements.planned_quantity.required,false);
  editor.elements.operation_kind.value='direct_forward';editor.elements.operation_kind.dispatchEvent(new p.w.Event('change',{bubbles:true}));assert.equal(editor.elements.planned_quantity.required,true);assert.equal(editor.elements.planned_unit.required,true);assert.equal(editor.checkValidity(),false);
  p.d.querySelector('#cancel').click();await until(()=>!p.d.querySelector('dialog[open]'));p.d.querySelector('.ck-needs-tools button:nth-child(2)').click();await until(()=>p.d.querySelector('dialog[open]'),p.errors);assert.equal(editor.elements.operation_kind.value,'operation');assert.equal(editor.elements.planned_quantity.value,'');
  for(const [name,value]of Object.entries({title:'QA-DOM库存无预约',customer:'QA-DOM合成客户',supply_chain_no:'QA-DOM-STOCK-01',instructions:'整理后反馈，等客户预约'}))editor.elements[name].value=value;
  editor.requestSubmit();await until(()=>!p.d.querySelector('dialog[open]'),p.errors);const row=f.env.DB.raw.prepare("SELECT state FROM sop_records WHERE kind='need' AND json_extract(state,'$.title')='QA-DOM库存无预约'").get();assert.ok(row);const data=JSON.parse(row.state);assert.equal(data.status,'pending');assert.equal(data.source_type,'inventory');assert.deepEqual(data.links,[]);assert.equal(data.created_by,'测试负责人');assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inventory booking quantity uses its labelled plan unit; excess allocations roll back and corrected saves remain linked',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';const p=await f.page('/002/?tab=need');
 try{await until(()=>p.d.querySelector('.ck-needs-tools'),p.errors);p.d.querySelector('.ck-needs-tools button:nth-child(2)').click();await until(()=>p.d.querySelector('dialog[open] [name=title]'),p.errors);const editor=p.d.querySelector('#editor');
  for(const [name,value]of Object.entries({title:'QA-DOM库存预约',customer:'QA-DOM合成客户',supply_chain_no:'QA-DOM-STOCK-02',instructions:'三托整理，预约两托',planned_quantity:'3',planned_unit:'托'}))editor.elements[name].value=value;
  await new Promise(r=>setTimeout(r,0));assert.match(p.d.querySelector('#optionalOutbounds>button').textContent,/客户已约出库/);assert.ok(!p.d.querySelector('#optionalOutbounds>button').textContent.includes('待安排'));
  p.d.querySelector('#optionalOutbounds>button').click();await new Promise(r=>setTimeout(r,0));assert.equal(editor.elements.planned_quantity.required,true);assert.match(p.d.querySelector('[data-ob-unit]').textContent,/托/);assert.equal(editor.querySelectorAll('[data-ob=expected_ship_at]').length,1);assert.match(editor.querySelector('[data-outbound-row]>legend').textContent,/已预约出库/);assert.match(editor.querySelector('[data-outbound-row]>button').textContent,/删除此出库计划/);
  for(const [name,value]of Object.entries({quantity:'4',expected_ship_at:'2026-10-05',outbound_mode:'customer_pickup'}))editor.querySelector('[data-ob='+name+']').value=value;
  editor.requestSubmit();await until(()=>p.d.querySelector('#formError').textContent,p.errors);assert.match(p.d.querySelector('#formError').textContent,/超过/);assert.equal(editor.querySelector('[data-ob=quantity]').value,'4');assert.equal(f.env.DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need' AND json_extract(state,'$.title')='QA-DOM库存预约'").get().n,0);
  editor.querySelector('[data-ob=quantity]').value='2';editor.requestSubmit();await until(()=>!p.d.querySelector('dialog[open]'),p.errors);const n=JSON.parse(f.env.DB.raw.prepare("SELECT state FROM sop_records WHERE kind='need' AND json_extract(state,'$.title')='QA-DOM库存预约'").get().state);assert.equal(n.links.length,1);assert.equal(n.links[0].unit,'托');const ob=f.env.DB.raw.prepare('SELECT * FROM v2_outbound_orders WHERE id=?').get(n.links[0].outbound_id);assert.equal(ob.planned_pallet_count,2);assert.equal(ob.planned_box_count,0);assert.equal(ob.instruction,n.instructions);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inventory-plan duplicate submit and lost-response retry create one requirement with one request id',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';const p=await f.page('/002/?tab=need');
 try{await until(()=>p.d.querySelector('.ck-needs-tools'),p.errors);p.d.querySelector('.ck-needs-tools button:nth-child(2)').click();await until(()=>p.d.querySelector('dialog[open] [name=title]'),p.errors);const editor=p.d.querySelector('#editor');for(const [name,value]of Object.entries({title:'QA-DOM库存重试',customer:'QA-DOM合成客户',supply_chain_no:'QA-DOM-STOCK-03',instructions:'整理库存'}))editor.elements[name].value=value;
  const original=p.w.fetch,ids=[];let lost=true;p.w.fetch=async(url,options)=>{const b=JSON.parse(options.body);if(b.action==='sop_need_create'){ids.push(b.client_req_id);await new Promise(r=>setTimeout(r,10));}const response=await original(url,options);if(b.action==='sop_need_create'&&lost){lost=false;throw new p.w.TypeError('QA lost response');}return response;};
  editor.requestSubmit();editor.requestSubmit();await until(()=>p.d.querySelector('#formError').textContent,p.errors);assert.equal(ids.length,1);assert.equal(editor.elements.title.value,'QA-DOM库存重试');editor.requestSubmit();await until(()=>!p.d.querySelector('dialog[open]'),p.errors);assert.equal(ids.length,2);assert.equal(ids[0],ids[1]);assert.equal(f.env.DB.raw.prepare("SELECT count(*) n FROM sop_records WHERE kind='need' AND json_extract(state,'$.title')='QA-DOM库存重试'").get().n,1);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
async function loadOrders(f,{extra=false}={}){
 f.env.SOP_WORK_CHAIN_ENABLED='true';const orders=[];
 const call=async(action,data={})=>{const r=await(await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();assert.equal(r.ok,true,action+': '+r.error);return r;};
 for(let i=0;i<(extra?5:3);i++){
  const customer='DOM装货客户 '+i,quantity=i===1?2:5+i,unit=i===1?'托':'箱';
  const n=await call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'LOAD-DOM-'+i,title:'DOM装货计划',customer,instructions:'按纸质单装货',planned_quantity:quantity,planned_unit:unit});
  const o=await call('v2_outbound_order_create',{customer,biz_class:'bulk',outbound_mode:'customer_pickup',expected_ship_at:'2026-10-03',sop_existing_need_id:n.id,sop_need_revision:n.revision,sop_link_quantity:quantity});await call('v2_outbound_order_update_status',{id:o.id,status:'issued'});
  if(i!==4){const staff=[{id:'PREP-DOM-'+i,name:'前序操作員'}],j=await call('sop_task_create',{department:'bulk',need_id:n.id,title:'DOM前序',job_type:'bulk_op',workers:staff,lead_id:staff[0].id,estimated_minutes:10});await call('sop_task_start',{id:j.id,revision:j.revision});const current=(await call('sop_get',{id:j.id})).record;await call('sop_task_complete_review',{id:j.id,revision:current.revision,result:{quantity,unit},decision:'pass',reason:'DOM验收'});}
  orders.push({...o,customer,box:i===1?0:quantity,pallet:i===1?2:0});
 }
 return {orders,call};
}
const storageOf=w=>Object.fromEntries(Array.from({length:w.sessionStorage.length},(_,i)=>{const k=w.sessionStorage.key(i);return[k,w.sessionStorage.getItem(k)];}));
async function scanLoad(p,code){p.d.querySelector('#ck-load-code').value=code;p.d.querySelector('#ck-load-scan').requestSubmit();await new Promise(r=>setTimeout(r,35));}
async function staffLoad(p){await until(()=>!p.d.querySelector('#ck-load-start').disabled,p.errors);p.d.querySelector('#ck-load-start').click();await until(()=>p.d.querySelector('dialog[open] [data-staff-badge]'),p.errors);const form=p.d.querySelector('dialog[open] form');form.querySelector('[data-staff-badge]').value='LOAD-DOM-A|装货甲';form.querySelector('[data-staff-add]').click();form.querySelector('[data-staff-badge]').value='LOAD-DOM-B|装货乙';form.querySelector('[data-staff-add]').click();await until(()=>form.querySelectorAll('[data-staff-people] .badge').length===2,p.errors);form.requestSubmit();}
test('loading UI scans and multi-selects three orders, deduplicates, removes wrong picks, blocks bad orders and retains input after staff changes and reload',opts,async()=>{
 const f=await fixture(),{orders,call}=await loadOrders(f,{extra:true});let p=await f.page('/001/');
 try{
  p.w.goPage('outbound_load');await until(()=>!p.d.querySelector('#ck-load-entry').hidden,p.errors);
  await scanLoad(p,orders[0].display_no);await scanLoad(p,orders[0].display_no);assert.equal(p.d.querySelectorAll('#ck-load-selected article').length,1);assert.match(p.d.querySelector('#ck-load-message').textContent,/不会重复/);
  p.d.querySelector('#ck-load-lookup').open=true;await new Promise(r=>setTimeout(r,70));assert.ok(p.d.querySelector('[data-select="'+orders[1].id+'"]'),p.d.querySelector('#ck-load-message').textContent+'; '+p.d.querySelector('#ck-load-candidates').innerHTML);p.d.querySelector('[data-select="'+orders[1].id+'"]').click();await scanLoad(p,orders[2].display_no);await scanLoad(p,orders[3].display_no);
  p.d.querySelector('[data-remove="'+orders[3].id+'"]').click();assert.equal(p.d.querySelectorAll('#ck-load-selected article').length,3);
  await scanLoad(p,orders[4].display_no);assert.equal(p.d.querySelector('[data-select="'+orders[4].id+'"]').disabled,true);assert.match(p.d.querySelector('#ck-load-message').textContent,/审核/);assert.equal(p.d.querySelectorAll('#ck-load-selected article').length,3);
  for(const o of orders.slice(0,3))assert.ok(p.d.querySelector('#ck-load-selected').textContent.includes(o.customer));
  await staffLoad(p);await until(()=>p.d.querySelectorAll('#ck-load-working [data-result]').length===3,p.errors);const job=p.w._activeJobId;
  const row=p.d.querySelector('#ck-load-working [data-result="'+orders[0].id+'"]');row.querySelector('[name=box]').value='4';row.querySelector('[name=box]').dispatchEvent(new p.w.Event('input',{bubbles:true}));
  await call('sop_native_people',{job_id:job,revision:1,workers:[{id:'LOAD-DOM-A',name:'装货甲'},{id:'LOAD-DOM-C',name:'装货丙'}],lead_id:'LOAD-DOM-A'});const refreshed=await p.w.api({action:'v2_ops_job_detail',job_id:job});p.w.dispatchEvent(new p.w.CustomEvent('ck-native-people-changed',{detail:refreshed}));
  await until(()=>p.d.querySelector('[data-working-crew]').textContent.includes('装货丙'),p.errors);assert.equal(p.d.querySelector('[data-result="'+orders[0].id+'"] [name=box]').value,'4');
  const saved=storageOf(p.w);assert.deepEqual(p.errors,[]);p.w.close();p=await f.page('/001/',saved);await until(()=>p.d.querySelectorAll('#ck-load-working [data-result]').length===3,p.errors);assert.equal(p.w._activeJobId,job);assert.equal(p.d.querySelector('[data-result="'+orders[0].id+'"] [name=box]').value,'4');
  for(const o of orders.slice(0,3)){const r=p.d.querySelector('[data-result="'+o.id+'"]');r.querySelector('[data-planned]').click();}
  p.d.querySelector('#ck-load-working form').requestSubmit();await until(()=>p.d.querySelector('#page-home.active'),p.errors);assert.equal(p.w._activeJobId,null);for(const o of orders.slice(0,3)){const d=await call('v2_outbound_order_detail',{id:o.id});assert.equal(d.order.status,'shipped');assert.equal(d.order.actual_box_count,o.box);assert.equal(d.order.actual_pallet_count,o.pallet);}assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('loading UI replays the frozen start request after a lost response and reload without scanning staff or creating another job',opts,async()=>{
 const f=await fixture(),{orders}=await loadOrders(f);let p=await f.page('/001/');try{
  p.w.goPage('outbound_load');await until(()=>!p.d.querySelector('#ck-load-entry').hidden,p.errors);for(const o of orders)await scanLoad(p,o.display_no);
  const original=p.w.fetch;let lost=false;p.w.fetch=async(url,options)=>{const r=await original(url,options);if(!lost&&JSON.parse(options.body).action==='sop_native_start'){lost=true;throw Error('fixture lost start response');}return r;};
  await staffLoad(p);await until(()=>/重试本次派工/.test(p.d.querySelector('#ck-load-start').textContent)&&!p.d.querySelector('#ck-load-start').disabled,p.errors);assert.equal(f.env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_load_trips').get().n,1);
  const saved=storageOf(p.w);p.w.close();p=await f.page('/001/',saved);p.w.goPage('outbound_load');await until(()=>/重试本次派工/.test(p.d.querySelector('#ck-load-start').textContent)&&!p.d.querySelector('#ck-load-start').disabled,p.errors);p.d.querySelector('#ck-load-start').click();await until(()=>p.d.querySelectorAll('#ck-load-working [data-result]').length===3,p.errors);assert.equal(p.d.querySelector('dialog[open]'),null);assert.equal(f.env.DB.raw.prepare('SELECT COUNT(*) n FROM ck_load_trips').get().n,1);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('lost finish response preserves frozen quantities; retry and reload never duplicate results or clocks',opts,async()=>{
 const f=await fixture(),{orders,call}=await loadOrders(f),p=await f.page('/001/');try{
  p.w.goPage('outbound_load');await until(()=>!p.d.querySelector('#ck-load-entry').hidden,p.errors);for(const o of orders)await scanLoad(p,o.display_no);await staffLoad(p);await until(()=>p.d.querySelectorAll('#ck-load-working [data-result]').length===3,p.errors);const job=p.w._activeJobId;
  p.d.querySelectorAll('#ck-load-working [data-planned]').forEach(b=>b.click());const original=p.w.fetch;let lost=false;p.w.fetch=async(url,options)=>{const r=await original(url,options);if(!lost&&JSON.parse(options.body).action==='v2_outbound_load_finish'){lost=true;throw Error('fixture lost finish response');}return r;};
  p.d.querySelector('#ck-load-working form').requestSubmit();await until(()=>/lost finish/.test(p.d.querySelector('[data-finish-message]').textContent),p.errors);assert.equal(p.d.querySelector('#ck-load-working [name=box]').disabled,true);assert.equal(f.env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(job).n,1);
  p.d.querySelector('#ck-load-working form').requestSubmit();await until(()=>p.d.querySelector('#page-home.active'),p.errors);assert.equal(f.env.DB.raw.prepare('SELECT COUNT(*) n FROM v2_ops_job_results WHERE job_id=?').get(job).n,1);assert.equal((await call('v2_ops_job_detail',{job_id:job})).workers.filter(w=>!w.left_at).length,0);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('original four modules initialize under shared session without old login prompts',opts,async()=>{
 const f=await fixture();for(const app of ['/','/001/','/002/','/003/','/shuju/']){const p=await f.page(app);try{
 assert.equal(p.d.querySelector('#ck-auth-gate'),null);assert.deepEqual(p.errors,[],app);
 if(app==='/002/')assert.equal(p.d.querySelector('#page-main').classList.contains('active'),true);
 if(app==='/001/')assert.equal(p.d.querySelector('#page-home').classList.contains('active'),true);
 }finally{p.w.close();}}
});
test('original collaboration buttons open integrated needs and verification editor',opts,async()=>{
 const f=await fixture(),p=await f.page('/002/');try{
 p.d.querySelector('[data-tab=need]').click();await until(()=>p.d.querySelector('#view-need article'),p.errors);assert.match(p.d.querySelector('#view-need').textContent,/虚拟|验收/);
 await p.w.openInboundDetail(f.seed.inbound_id);await until(()=>p.d.querySelector('#inboundDetailBody .ck-inline-heading button'),p.errors);
 p.d.querySelector('#inboundDetailBody .ck-inline-heading button').click();await until(()=>p.d.querySelector('#view-need #groupTable'),p.errors);
 p.d.querySelector('#btnNewCheck').click();await until(()=>p.d.querySelector('#checkListBody dialog')?.open,p.errors);
 assert.ok(p.d.querySelector('#checkListBody input[name=ship_date]'));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('original field and dashboard integrate added controls',opts,async()=>{
 const f=await fixture(),p=await f.page('/001/');try{
 assert.ok(!p.d.querySelector('.home-grid').textContent.includes('负责人派工与审核'));
 p.w.goPage('order_op_menu');p.d.querySelector('#page-order_op_menu button[onclick="goPage(\'bulk_op\')"]').click();await until(()=>p.d.querySelector('#page-bulk_op [data-scanform]'),p.errors);
 assert.ok(p.d.querySelector('#page-bulk_op').classList.contains('active'));
 p.d.querySelector('#page-bulk_op [data-mode=external]').click();await until(()=>/外部/.test(p.d.querySelector('#page-bulk_op [data-code]')?.placeholder),p.errors);assert.equal(p.d.querySelector('#bulkStateIdle').style.display,'none');assert.equal(p.d.querySelector('#bulkStateWorking').style.display,'none');assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
 const q=await f.page('/shuju/');try{Array.from(q.d.querySelectorAll('.tab-bar button')).find(x=>x.textContent==='派工质量与待办').click();await until(()=>q.d.querySelector('#ck-dashboard article'),q.errors);assert.deepEqual(q.errors,[]);}finally{q.w.close();}
});
test('ongoing bulk work cards show each customer and scanned work-plan number',opts,async()=>{
 const f=await fixture(),call=async(action,data)=>(await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();
 const orders=[];
 for(let i=0;i<2;i++){
  const customer=i===0?'Fixture <&>':'另一个客户';
  const need=await call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'STOCK-'+i,title:'打托',customer,instructions:'打托后上架',planned_quantity:2,planned_unit:'托'});assert.equal(need.ok,true,need.error);
  const task=await call('sop_task_create',{department:'bulk',title:'打托',need_id:need.id,job_type:'bulk_op',estimated_minutes:30,workers:[{id:'W-'+i,name:'操作员 '+i}],lead_id:'W-'+i});assert.equal(task.ok,true,task.error);
  const detail=await call('sop_get',{id:need.id});orders.push({id:task.id,number:detail.record.display_no,customer});
 }
 const p=await f.page('/001/');try{
  p.w.goPage('bulk_op');await until(()=>p.d.querySelectorAll('#page-bulk_op [data-task]').length>=2,p.errors);
  for(const order of orders){const card=p.d.querySelector('#page-bulk_op [data-task="'+order.id+'"]');assert.equal(card.querySelector('.ck-task-number').textContent,order.number);assert.equal(card.querySelector('.ck-task-customer').textContent,'客户 / 고객：'+order.customer);assert.match(card.textContent,/打托/);}
  p.d.querySelector('#page-bulk_op [data-task="'+orders[0].id+'"]').click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number'),p.errors);
  assert.match(p.d.querySelector('#page-bulk_op .ck-work-number').textContent,new RegExp(orders[0].number));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('work files remain available on office PC and do not render or load on field pages',opts,async()=>{
 const f=await fixture();f.env.SOP_WORK_CHAIN_ENABLED='true';
 const created=await (await f.request({action:'sop_need_create',department:'bulk',source_type:'inventory',supply_chain_no:'STOCK-MATERIALS',title:'打托',customer:'Fixture',instructions:'打托后反馈',planned_quantity:2,planned_unit:'托',client_req_id:crypto.randomUUID()})).json();assert.equal(created.ok,true,created.error);
 const need=(await (await f.request({action:'sop_get',id:created.id})).json()).record;
 f.env.DB.raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_name,file_key,uploaded_by,created_at) VALUES(?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?)').run('FIELD-FILE','sop_need',need.id,'work_material','仓库实际明细.pdf','field.pdf','现场人员','2026-09-30T10:00:00Z','OFFICE-FILE','sop_need',need.id,'pallet_label','托唛.pdf','office.pdf','客服','2026-09-30T10:01:00Z');
 const p=await f.page('/002/');try{
  const host=p.d.createElement('section');p.d.body.append(host);
  await p.w.CKWorkChain.materials(host,need);
  const office=host.querySelector('[data-direction=office]'),field=host.querySelector('[data-direction=field]');
  assert.match(office.textContent,/托唛.pdf/);assert.doesNotMatch(office.textContent,/仓库实际明细.pdf/);
  assert.match(field.textContent,/仓库实际明细.pdf/);assert.doesNotMatch(field.textContent,/托唛.pdf/);
  assert.deepEqual([...office.querySelectorAll('[data-kind] option')].map(x=>x.value),['pallet_label','shipping_document','product_label']);
  assert.ok(field.querySelector('form'),'office can record warehouse feedback on behalf of the field');
  assert.ok(field.querySelector('input[type=file]'),'office feedback has a file picker');
  assert.match(field.querySelector('button[type=submit]').textContent,/代录仓库反馈/);
  assert.equal(field.querySelector('[data-kind]'),null,'office feedback cannot be mislabeled as a customer document');
  p.w.toggleLang();assert.match(office.textContent,/고객 담당자가 창고에 제공하는 작업 자료/);assert.match(field.textContent,/창고에서 고객 담당자에게 전달하는 작업 설명/);
  let fileReads=0;const request=p.w.CKSession.request;p.w.CKSession.request=(action,...args)=>{if(['sop_work_materials','sop_batch_work_materials'].includes(action))fileReads++;return request(action,...args);};
  await p.w.CKWorkChain.materials(host,need,{field:true});
  await p.w.CKWorkChain.mountNeed(host,need,{field:true});
  await p.w.CKWorkChain.batchMaterials(host,{source_type:'inbound',items:[need]},{field:true});
  assert.equal(host.innerHTML,'','field context has no customer files or feedback form');assert.equal(fileReads,0,'field context never requests work files');
  assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
 const q=await f.page('/001/?need='+encodeURIComponent(need.id));try{
  await until(()=>q.d.querySelector('#page-bulk_op .ck-work-number'),q.errors);
  const sheet=q.d.querySelector('#page-bulk_op .ck-work-sheet');assert.equal(sheet.querySelector('.ck-instructions').textContent,need.instructions);
  assert.equal(q.d.querySelector('#page-bulk_op .chain-material-group'),null);assert.equal(q.d.querySelector('#page-bulk_op .chain-upload'),null);
  const host=q.d.createElement('section');q.d.body.append(host);await q.w.CKWorkChain.materials(host,need);assert.equal(host.innerHTML,'','field URL omits files even without an explicit field option');
  assert.deepEqual(q.errors,[]);
 }finally{q.w.close();}
});

test('home opens both work-plan and external jobs in the shared workspace; finishing cannot leak outputs into the next job',opts,async()=>{
 const f=await fixture(),call=async(action,data={})=>{const r=await (await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();assert.equal(r.ok,true,r.error);return r;};
 const external=[];
 for(let i=0;i<2;i++)external.push(await call('sop_native_start',{payload:{action:'v2_bulk_op_job_start',work_order_no:'EXT-'+i,customer:'外部客户 '+i,client_req_id:crypto.randomUUID()},workers:[{id:'EXT-W-'+i,name:'外部员工 '+i}],lead_id:'EXT-W-'+i,estimated_minutes:30,labor_department:'bulk'}));
 const need=await call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'MIXED-HOME',title:'打托',customer:'计划客户',instructions:'28箱打成2托',planned_quantity:2,planned_unit:'托'});
 const task=await call('sop_task_dispatch',{department:'bulk',need_id:need.id,title:'打托',job_type:'bulk_op',estimated_minutes:30,workers:[{id:'PLAN-W',name:'计划员工'}],lead_id:'PLAN-W'}),number=(await call('sop_get',{id:need.id})).record.display_no;
 const p=await f.page('/001/');
 const cards=()=>[...p.d.querySelectorAll('#page-home .ck-native-task')],card=no=>cards().find(b=>b.querySelector('strong').textContent===no),action=prefix=>[...p.d.querySelectorAll('#page-bulk_op [data-actions] button')].find(b=>b.textContent.startsWith(prefix)),status=id=>f.env.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(id).status;
 const openFinish=async()=>{action('填写产出').click();await until(()=>p.d.querySelector('#page-bulk_op [data-finish]'),p.errors);return p.d.querySelector('#page-bulk_op [data-finish]');};
 try{
  await until(()=>card(number)&&card('EXT-0')&&card('EXT-1'),p.errors);assert.match(card(number).textContent,/计划客户/);assert.match(card('EXT-0').textContent,/外部客户 0/);
  card(number).click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent===number,p.errors);
  assert.equal(p.d.querySelector('#page-bulk_op [data-mode=need]').getAttribute('aria-pressed'),'true');
  let form=await openFinish();form.elements.quantity.value='2';form.elements.unit.value='托';form.elements.pallet_count.value='2';form.elements.operated_box_count.value='28';form.elements.reason.value='核对完成';
  const original=p.w.CKSession.request;let failOnce=true;p.w.CKSession.request=(action,...args)=>{if(action==='sop_task_complete_review'&&failOnce){failOnce=false;return Promise.reject(Object.assign(Error('虚拟审核失败'),{businessError:true}));}return original(action,...args);};
  form.requestSubmit();await until(()=>p.d.querySelector('#page-bulk_op [data-error]')?.textContent==='虚拟审核失败',p.errors);
  assert.equal(status(task.id),'working');assert.ok(p.d.querySelector('#page-bulk_op').classList.contains('active'));assert.equal(form.elements.operated_box_count.value,'28');assert.equal(form.querySelector('button').disabled,false);
  form.requestSubmit();await until(()=>status(task.id)==='completed',p.errors);await until(()=>p.d.querySelector('#page-home').classList.contains('active')&&card('EXT-0')&&!card(number),p.errors);
  assert.equal(p.w._activeJobId,null);assert.equal(p.w._navStack.length,0);assert.equal(Object.keys(p.w._pageParams).length,0);
  card('EXT-0').click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent==='EXT-0',p.errors);
  assert.equal(p.d.querySelector('#bulkStateWorking').style.display,'none');assert.equal(p.d.querySelector('#page-bulk_op [data-mode=external]').getAttribute('aria-pressed'),'true');
  assert.match(p.d.querySelector('#page-bulk_op [data-content]').textContent,/外部员工 0/);
  form=await openFinish();assert.equal(form.elements.customer.value,'外部客户 0');assert.equal(form.elements.pallet_count.value,'0');assert.equal(form.elements.pallet_count.disabled,false);
  form.elements.pallet_count.value='3';form.elements.packed_count.value='4';form.elements.operated_box_count.value='28';form.elements.reason.value='外部单核对完成';form.requestSubmit();
  await until(()=>status(external[0].job_id)==='completed',p.errors);await until(()=>p.d.querySelector('#page-home').classList.contains('active')&&card('EXT-1')&&!card('EXT-0'),p.errors);
  assert.equal(p.w._activeJobId,null);assert.equal(p.w._navStack.length,0);
  const result=JSON.parse(f.env.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(external[0].job_id).result_json);
  assert.equal(result.packed_box_count,4);assert.equal(result.pallet_count,3);assert.equal(result.total_operated_box_count,28);assert.equal(result.result_note,'外部单核对完成');
  assert.equal(f.env.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(external[0].job_id).n,0);
  card('EXT-1').click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent==='EXT-1',p.errors);
  form=await openFinish();assert.equal(form.elements.customer.value,'外部客户 1');assert.equal(form.elements.pallet_count.value,'0');assert.equal(form.elements.packed_count.value,'0');assert.equal(form.elements.reason.value,'');assert.equal(form.elements.pallet_count.disabled,false);
  assert.equal(p.d.querySelector('#page-bulk_op .chain-material-group'),null);assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});

test('reviewing saved work returns home only on approval; correction stays on the task',opts,async()=>{
 const f=await fixture(),call=async(action,data={})=>{const r=await (await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();assert.equal(r.ok,true,r.error);return r;},orders=[];
 for(const decision of ['return','pass']){
  const need=await call('sop_need_create',{department:'bulk',source_type:'inventory',supply_chain_no:'REVIEW-'+decision,title:'审核测试',customer:'审核客户',instructions:'核对一箱',planned_quantity:1,planned_unit:'箱'});
  const task=await call('sop_task_dispatch',{department:'bulk',need_id:need.id,title:'审核测试',job_type:'bulk_op',estimated_minutes:30,workers:[{id:'REVIEW-W-'+decision,name:'审核员工'}],lead_id:'REVIEW-W-'+decision});
  await call('sop_task_finish',{id:task.id,revision:task.revision,result:{quantity:1,unit:'箱'}});orders.push({id:task.id,decision,number:(await call('sop_get',{id:need.id})).record.display_no});
 }
 const p=await f.page('/001/');try{
  for(const order of orders){
   if(order.decision==='pass')p.w.goPage('home');
   const card=()=>[...p.d.querySelectorAll('#page-home .ck-native-task')].find(b=>b.querySelector('strong').textContent===order.number);
   await until(()=>card(),p.errors);card().click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent===order.number,p.errors);
   [...p.d.querySelectorAll('#page-bulk_op [data-actions] button')].find(b=>b.textContent.startsWith('审核原有')).click();
   const form=p.d.querySelector('#page-bulk_op [data-review]');form.elements.decision.value=order.decision;form.elements.reason.value='现场核对';form.requestSubmit();
   const status=()=>f.env.DB.raw.prepare('SELECT status FROM v2_ops_jobs WHERE id=?').get(order.id).status;
   await until(()=>status()===(order.decision==='pass'?'completed':'pending'),p.errors);
   if(order.decision==='pass'){await until(()=>p.d.querySelector('#page-home').classList.contains('active')&&!card(),p.errors);assert.equal(p.w._navStack.length,0);}
   else{await until(()=>p.d.querySelector('#page-bulk_op .ck-state')?.textContent.startsWith('待整改'),p.errors);assert.ok(p.d.querySelector('#page-bulk_op').classList.contains('active'));}
  }
  assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});

test('external scanner creates a crew once, resumes by external number and pauses without losing task history',opts,async()=>{
 const f=await fixture(),p=await f.page('/001/');try{
  p.w.goPage('bulk_op',{external:true});await until(()=>p.d.querySelector('#page-bulk_op [data-code]'),p.errors);
  const scan=async code=>{const input=p.d.querySelector('#page-bulk_op [data-code]');input.value=code;input.form.requestSubmit();};
  await scan('EMP-INVALID|人员');await until(()=>p.d.querySelector('#page-bulk_op [data-error]')?.textContent,p.errors);assert.equal(f.env.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_jobs WHERE job_type='bulk_op'").get().n,0);
  await scan('NEW-EXTERNAL');await until(()=>p.d.querySelector('#page-bulk_op [data-people]'),p.errors);
  const form=p.d.querySelector('#page-bulk_op [data-people]');form.elements.customer.value='新增外部客户';form.querySelector('[data-staff-badge]').value='NEW-W|新员工';form.querySelector('[data-staff-add]').click();form.requestSubmit();
  await until(()=>p.d.querySelector('#page-bulk_op [data-actions]'),p.errors);
  const job=f.env.DB.raw.prepare("SELECT id FROM v2_ops_jobs WHERE related_doc_id='NEW-EXTERNAL'").get(),segments=()=>f.env.DB.raw.prepare('SELECT * FROM v2_ops_job_workers WHERE job_id=?').all(job.id);
  assert.equal(segments().length,1);const joined=segments()[0].joined_at;
  p.d.querySelector('#page-bulk_op [data-back]').click();await until(()=>p.d.querySelector('#page-bulk_op [data-code]'),p.errors);await scan('NEW-EXTERNAL');await until(()=>p.d.querySelector('#page-bulk_op [data-actions]'),p.errors);
  assert.equal(segments().length,1);assert.equal(segments()[0].joined_at,joined);assert.equal(segments()[0].left_at,'');
  [...p.d.querySelectorAll('#page-bulk_op [data-actions] button')].find(b=>b.textContent.startsWith('暂停')).click();const pause=p.d.querySelector('#page-bulk_op [data-pause]');pause.elements.reason.value='人员换班';pause.requestSubmit();
  await until(()=>p.d.querySelector('#page-bulk_op .ck-state')?.textContent.includes('待收尾'),p.errors);assert.ok(segments()[0].left_at);
  [...p.d.querySelectorAll('#page-bulk_op [data-actions] button')].find(b=>b.textContent.startsWith('核对人员')).click();const people=p.d.querySelector('#page-bulk_op [data-people]');people.querySelector('[data-staff-badge]').value='NEW-W|新员工';people.querySelector('[data-staff-add]').click();people.querySelector('[data-reason]').value='继续作业';people.requestSubmit();
  await until(()=>p.d.querySelector('#page-bulk_op .ck-state')?.textContent.startsWith('作业中'),p.errors);assert.equal(segments().length,2);assert.equal(segments()[0].joined_at,joined);assert.equal(segments()[1].left_at,'');assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
test('inbound work rows and inventory need optional outbound fields use original forms',opts,async()=>{
 const f=await fixture(),p=await f.page('/002/');try{
 p.w.goView('inbound_create');p.d.querySelector('[data-add-work]').click();
 const row=p.d.querySelector('[data-work-row]');row.querySelector('[data-work=title]').value='虚拟打托';row.querySelector('[data-work=instructions]').value='1–15打托';row.querySelector('[data-work=planned_quantity]').value='15';row.querySelector('[data-add-ob]').click();row.querySelector('[data-ob=expected_ship_at]').value='2026-09-25';row.querySelector('[data-ob=quantity]').value='15';row.querySelector('[data-ob=outbound_mode]').value='customer_pickup';row.querySelector('[data-ob=po_no]').value='PO-FIXTURE';
 const data=p.w.CKInboundWorks.read();assert.equal(data.length,1);assert.equal(data[0].outbounds[0].quantity,'15');
 p.d.querySelector('[data-tab=need]').click();await until(()=>p.d.querySelector('#view-need .ck-needs-tools button:nth-child(2)'),p.errors);p.d.querySelector('#view-need .ck-needs-tools button:nth-child(2)').click();await until(()=>p.d.querySelector('#view-need dialog').open,p.errors);
 assert.equal(p.d.querySelector('#view-need [name=source_type]').value,'inventory');assert.ok(p.d.querySelector('#view-need #optionalOutbounds button'));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});

test('one inbound instruction card contains every child and existing inbound print includes latest requirements',opts,async()=>{
 const f=await fixture();const call=async(action,data)=>(await f.request({action,client_req_id:crypto.randomUUID(),...data})).json();
 const added=await call('sop_need_create',{department:'bulk',source_type:'inbound',source_id:f.seed.inbound_id,customer:'虚拟验收客户',title:'另一分货要求',instructions:'第81至100箱保持散箱',owner:'测试处理员',reason:'分货明细',planned_quantity:20,planned_unit:'箱'});assert.equal(added.ok,true,added.error);
 const p=await f.page('/002/');try{
 p.d.querySelector('[data-tab=need]').click();await until(()=>p.d.querySelector('#view-need .ck-work-group'),p.errors);
 assert.equal(p.d.querySelectorAll('#view-need .ck-work-group').length,1);assert.match(p.d.querySelector('#view-need .ck-work-group').textContent,/2项作业/);
 p.d.querySelector('#view-need .ck-work-group button').click();await until(()=>p.d.querySelector('#groupTable'),p.errors);assert.equal(p.d.querySelectorAll('#groupTable tbody tr').length,2);
 await p.w.openInboundDetail(f.seed.inbound_id);
 const record=await call('sop_get',{id:added.id});const changed=await call('sop_need_update',{id:added.id,revision:record.record.revision,instructions:'最新要求：第81至100箱散箱，禁止打托',owner:'测试处理员'});assert.equal(changed.ok,true,changed.error);
 let printed='';p.w.open=()=>({document:{open(){printed='';},write(s){printed+=s;},close(){}},close(){},focus(){},print(){}});
 await p.w.printIbQr();assert.match(printed,/入库计划单/);assert.match(printed,/入库货物明细/);assert.match(printed,/本批作业要求及卸货分货依据/);assert.match(printed,/最新要求：第81至100箱散箱，禁止打托/);assert.match(printed,/class="inbound-cargo"/);assert.ok(!printed.includes('class="linked-ob-table"'));assert.deepEqual(p.errors,[]);
 }finally{p.w.close();}
});
