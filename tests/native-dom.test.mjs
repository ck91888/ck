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
 async function page(url){
  const errors=[];const {JSDOM,requestInterceptor,VirtualConsole}=runtime;const vc=new VirtualConsole();vc.on('jsdomError',e=>{if(!/Not implemented:|Could not parse CSS stylesheet/.test(e.message))errors.push(e.message)});
  // jsdom does not execute dynamic module imports; inject the actual module exports below.
  const resources={interceptors:[requestInterceptor(req=>{const u=new URL(req.url);if(u.origin!=='https://fixture.test')throw Error('External request rejected: '+u.origin);let source=fs.readFileSync(path.join(assets,u.pathname),'utf8');source=source.replace(/import\('\/shared\/([a-z-]+)\.js'\)/g,(_m,name)=>'Promise.resolve(window.__fixtureModules["'+name+'"])');return new Response(source,{headers:{'Content-Type':u.pathname.endsWith('.js')?'application/javascript':'text/css'}});})]};
  const u=new URL(url,'https://fixture.test');const html=fs.readFileSync(path.join(assets,u.pathname,'index.html'),'utf8');
  const dom=new JSDOM(html,{url:u.href,runScripts:'dangerously',resources,virtualConsole:vc,pretendToBeVisual:true,beforeParse(w){
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
  let form=await openFinish();form.elements.quantity.value='2';form.elements.unit.value='托';form.elements.pallet_count.value='2';form.elements.operated_box_count.value='28';form.elements.reason.value='核对完成';form.requestSubmit();
  await until(()=>status(task.id)==='completed',p.errors);await until(()=>/已完成/.test(p.d.querySelector('#page-bulk_op .ck-state')?.textContent),p.errors);
  p.w.goPage('home');await until(()=>card('EXT-0')&&!card(number),p.errors);
  card('EXT-0').click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent==='EXT-0',p.errors);
  assert.equal(p.d.querySelector('#bulkStateWorking').style.display,'none');assert.equal(p.d.querySelector('#page-bulk_op [data-mode=external]').getAttribute('aria-pressed'),'true');
  assert.match(p.d.querySelector('#page-bulk_op [data-content]').textContent,/外部员工 0/);
  form=await openFinish();assert.equal(form.elements.customer.value,'外部客户 0');assert.equal(form.elements.pallet_count.value,'0');assert.equal(form.elements.pallet_count.disabled,false);
  form.elements.pallet_count.value='3';form.elements.packed_count.value='4';form.elements.operated_box_count.value='28';form.elements.reason.value='外部单核对完成';form.requestSubmit();
  await until(()=>status(external[0].job_id)==='completed',p.errors);await until(()=>/已完成/.test(p.d.querySelector('#page-bulk_op .ck-state')?.textContent),p.errors);
  const result=JSON.parse(f.env.DB.raw.prepare('SELECT result_json FROM v2_ops_job_results WHERE job_id=?').get(external[0].job_id).result_json);
  assert.equal(result.packed_box_count,4);assert.equal(result.pallet_count,3);assert.equal(result.total_operated_box_count,28);assert.equal(result.result_note,'外部单核对完成');
  assert.equal(f.env.DB.raw.prepare("SELECT COUNT(*) n FROM v2_ops_job_workers WHERE job_id=? AND left_at=''").get(external[0].job_id).n,0);
  p.w.goPage('home');await until(()=>card('EXT-1')&&!card('EXT-0'),p.errors);card('EXT-1').click();await until(()=>p.d.querySelector('#page-bulk_op .ck-work-number b')?.textContent==='EXT-1',p.errors);
  form=await openFinish();assert.equal(form.elements.customer.value,'外部客户 1');assert.equal(form.elements.pallet_count.value,'0');assert.equal(form.elements.packed_count.value,'0');assert.equal(form.elements.reason.value,'');assert.equal(form.elements.pallet_count.disabled,false);
  assert.equal(p.d.querySelector('#page-bulk_op .chain-material-group'),null);assert.deepEqual(p.errors,[]);
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
