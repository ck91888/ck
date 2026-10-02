(function(){
'use strict';
window.CKWorkflow=function(root,options={}){
root.classList.add('ck-workflow');if(options.context==='collab')root.classList.add('ck-native-collab');
root.innerHTML=`<div id="notice" role="status" aria-live="polite"></div><div class="toolbar"><button id="refresh"><span data-i18n="ck_plan_4">刷新 / 새로고침</span></button><span id="updates"></span></div><section id="content"></section><dialog id="modal"><form id="editor"><h2 id="editorTitle"></h2><div id="fields"></div><div class="toolbar"><button type="submit" id="save"><span data-i18n="ck_plan_5">保存 / 저장</span></button><button type="button" id="cancel" class="light"><span data-i18n="ck_plan_6">关闭 / 닫기</span></button></div><p id="formError" role="alert"></p></form></dialog>`;
const $=id=>root.querySelector('[id="'+id+'"]'), esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const copy=window.CKPlanCopy,mark=value=>copy?copy.html(value):esc(value);
const officeFiles=options.context!=='field'&&window.CKSession?.user?.scope!=='field'&&!window.location.pathname.startsWith('/001/');
for(const [id,label] of [['refresh','刷新 / 새로고침'],['save','保存 / 저장'],['cancel','关闭 / 닫기']])if(copy)copy.bind($(id),label);
const entry=new URLSearchParams(options);
const labels={need:'作业计划 / 작업 계획',task:'负责人派工 / 작업 배정',issue:'问题沟通 / 이슈',check:'出库核对 / 출고 확인',dashboard:'管理看板 / 관리 현황'};
const departments={bulk:'大货 / 대량',direct_ship:'代发 / 출고대행',import:'进口 / 수입'};
const states={pending:'待安排',assigned:'已分配',working:'作业中',paused:'已暂停',awaiting_review:'待审核',rework:'待整改',completed:'审核通过',waiting_customer:'待客户安排',linked:'已关联出库',closed:'已关闭',cancelled:'已作废',open:'处理中',responded:'已反馈'};
 let user=window.CKSession?.user,tab=Object.hasOwn(labels,entry.get('tab'))?entry.get('tab'):'need',offset=0,current=null,scanner=null,staffPicker=null,poll=null,listSnapshot='',currentGroup='',planObserver=null;
async function api(action,data={}){const result=await window.CKSession.request(action,data);if(destroyed)throw Error('页面已切换');return result;}
function notice(s){if(!destroyed)$('notice').textContent=s;}
function btn(label,fn,cls=''){const b=document.createElement('button');if(copy)copy.bind(b,label);else b.textContent=label;b.className=cls;b.type='button';b.onclick=()=>Promise.resolve().then(fn).catch(e=>notice(e.message));return b;}
function input(name,label,type='text',value='',required=true){return `<label>${mark(label)}<input name="${name}" type="${type}" value="${esc(value)}" ${required?'required':''}></label>`;}
function area(name,label,value='',required=true){return `<label>${mark(label)}<textarea name="${name}" ${required?'required':''}>${esc(value)}</textarea></label>`;}
function select(name,label,values,value=''){return `<label>${mark(label)}<select name="${name}">${Object.entries(values).map(([k,v])=>`<option${copy?copy.attrs(v):''} value="${esc(k)}" ${k===value?'selected':''}>${esc(copy?copy.text(v):v)}</option>`).join('')}</select></label>`;}
function depInput(){const ds=user.role==='manager'?departments:Object.fromEntries((user.departments||[]).map(k=>[k,departments[k]||k]));return select('department','部门 / 부서',ds);}
 async function closeModal(){planObserver?.disconnect();planObserver=null;const modal=$('modal'),picker=staffPicker;staffPicker=null;if(picker)await picker.destroy();if(scanner){await scanner.stop().catch(()=>{});scanner=null;}modal?.close();}
// Retry the identical request after a transport error. Never create a second mutation id.
function form(title,html,action,build,after){
  planObserver?.disconnect();planObserver=null;$('editor').oninput=null;$('editor').onchange=null;$('modal').classList.toggle('ck-plan-editor',action==='sop_need_create'||action==='sop_need_from_outbound');
  if(copy)copy.bind($('editorTitle'),title);else $('editorTitle').textContent=title;$('fields').innerHTML=html;$('formError').textContent='';$('save').disabled=false;$('cancel').disabled=false;
  let pending=null,pendingValues=null,submitting=false;const revision=current?.revision,id=current?.id;
  $('editor').onsubmit=async e=>{e.preventDefault();if(submitting)return;submitting=true;$('save').disabled=true;$('cancel').disabled=true;
 try{const values=Object.fromEntries(new FormData($('editor'))),valueKey=JSON.stringify(values);if(valueKey!==pendingValues){pending=null;pendingValues=valueKey;}if(!pending){for(const el of $('editor').elements){if(!el._ckSources||!values[el.name])continue;const selected=el._ckSources.find(x=>x.id===values[el.name]||(x.number||x.label.split(' / ')[0])===values[el.name]);if(!selected)throw Error('请从列表选择有效单据');values[el.name]=selected.id;}pending={...await build(values),client_req_id:crypto.randomUUID()};if(id&&!pending.id&& !action.endsWith('_create')&&action!=='sop_issue_adopt'){pending.id=id;pending.revision=revision;}}
   const result=await api(action,pending);await closeModal();await(after?after(result):id?detail(id):load());if(current?.kind==='issue'&&options.onSaved)await options.onSaved(result);notice('已保存 / 저장 완료');
 }catch(e){$('formError').textContent=e.message; // Business errors return a response: allow corrections with a new request.
  if(!(e instanceof TypeError)){pending=null;}
   }finally{submitting=false;const save=$('save'),cancel=$('cancel');if(save)save.disabled=false;if(cancel)cancel.disabled=false;}};
 $('modal').showModal();
}
$('cancel').onclick=closeModal;
 $('modal').addEventListener('cancel',e=>{e.preventDefault();if(!$('save').disabled)closeModal();});
 $('refresh').onclick=()=> (options.onRefresh?options.onRefresh():current?detail(current.id,true):currentGroup?groupDetail({group_key:currentGroup}):load()).catch(e=>notice(e.message));
function renderTabs(){}
let checkingUpdates=false;
async function checkUpdates(){if(checkingUpdates)return;checkingUpdates=true;try{await checkUpdatesOnce();}finally{checkingUpdates=false;}}
async function checkUpdatesOnce(){if(tab==='dashboard'&&user&&!document.hidden&&root.isConnected&&root.offsetParent!==null){await renderDashboard();return;}if(!user||document.hidden||!root.isConnected||root.offsetParent===null||$('modal').open)return;try{if(current){const id=current.id,r=await api('sop_get',{id,version_only:true});if(current?.id===id&&r.record.revision!==current.revision)$('updates').textContent='有新要求/新记录，请刷新查看并确认 · 새 변경사항';}else if(tab==='need'){const r=await api('sop_need_groups',currentGroup?{group_key:currentGroup}:{offset});if(JSON.stringify(r.items.map(g=>[g.key,g.items.map(x=>[x.id,x.revision])]))!==listSnapshot)$('updates').textContent='本批作业有更新，请刷新查看最新要求';}else if(tab!=='dashboard'){const r=await api('sop_list',{kind:tab,offset});if(JSON.stringify(r.items.map(x=>[x.id,x.revision]))!==listSnapshot)$('updates').textContent='列表有更新，请刷新 · 업데이트';}}catch{} }
async function load(){notice('');$('updates').textContent='';current=null;currentGroup='';const c=$('content');c.innerHTML='<p>加载中…</p>';if(tab==='dashboard')return renderDashboard();if(tab==='need')return loadGroups();
 const r=await api('sop_list',{kind:tab,offset});listSnapshot=JSON.stringify(r.items.map(x=>[x.id,x.revision]));c.innerHTML='';const bar=document.createElement('div');bar.className='toolbar';
 const createLabels={need:'新建作业计划',task:'新建派工任务',check:'按出库日期建清单',issue:'接入现有问题'};
 if(user.role!=='viewer')bar.append(btn(createLabels[tab],()=>create(tab)));
 if(offset)bar.append(btn('上一页',()=>{offset=Math.max(0,offset-50);return load();},'light'));
 if(r.more)bar.append(btn('下一页',()=>{offset+=50;return load();},'light'));c.append(bar);
 if(!r.items.length)c.insertAdjacentHTML('beforeend','<div class="card muted">暂无记录。请从关联单据建立作业，或点击上方按钮。</div>');
 for(const x of r.items){const a=document.createElement('article');a.innerHTML=`<h3>${esc(x.title)} <span class="status">${mark(states[x.status]||x.status)}</span></h3><p>${mark(departments[x.department])} · ${esc(x.owner||'')} · ${esc(x.location||'')}</p><p class="muted">${esc(window.CKDocumentLabels.number(x))} · 版本 ${x.revision}</p>${x.kind==='issue'&&x.requirement_version>x.ack_version?'<p class="warn">作业要求有变更，仓库待确认</p>':''}`;a.append(btn('打开 / 열기',()=>detail(x.id)));c.append(a);}
}
function sourceName(g){return g.source_type==='inbound'?'入库计划':g.source_type==='inventory'?'库内库存':'原业务单据';}
function groupHeader(g){return '<div class="card-title">'+esc(sourceName(g))+' · '+esc(g.display_no)+'</div><div class="ck-plan-facts"><div><span><span data-i18n="ck_plan_17">客户</span></span><strong>'+esc(g.customer)+'</strong></div><div><span>本批货物</span><strong>'+esc(g.cargo_summary||'见下方作业范围')+'</strong></div><div><span>预计到货</span>'+esc(g.expected_arrival||'—')+'</div></div>';}
async function loadGroups(){
 root.classList.add('ck-needs-list');
 const r=await api('sop_need_groups',{offset});listSnapshot=JSON.stringify(r.items.map(g=>[g.key,g.items.map(x=>[x.id,x.revision])]));const c=$('content');c.innerHTML='';
 const bar=document.createElement('div');bar.className='toolbar actions-bar ck-needs-tools';bar.append(btn('刷新 / 새로고침',()=>load(),'light'));if(user.role!=='viewer')bar.append(btn('＋库存作业计划',()=>create('need'),'light'));const hint=document.createElement('span');hint.className='muted';hint.textContent='共 '+r.total+' 批 · 第 '+(Math.floor(offset/50)+1)+' / '+Math.max(1,Math.ceil(r.total/50))+' 页';if(copy)copy.counts(hint,r.total,Math.floor(offset/50)+1,Math.max(1,Math.ceil(r.total/50)));bar.append(hint);if(offset)bar.append(btn('上一页',()=>{offset=Math.max(0,offset-50);return load();},'light'));if(r.more)bar.append(btn('下一页',()=>{offset+=50;return load();},'light'));c.append(bar);
 if(!r.items.length)c.insertAdjacentHTML('beforeend','<div class="card muted">'+mark('暂无作业计划。入库作业请在入库计划中填写；库存作业可单独新增。')+'</div>');
 for(const g of r.items){const card=document.createElement('article');card.className='card ck-work-group ck-need-row';const summary=g.items.filter(x=>x.status!=='cancelled').map(x=>x.title).join('；'),status=g.status==='completed'?'作业已完成':g.status==='working'?'处理中':g.status==='cancelled'?'已取消':'待安排';card.innerHTML='<div class="ck-need-main"><div class="ck-need-id" title="'+esc(sourceName(g))+'"><strong>'+esc(g.display_no)+'</strong></div><strong class="ck-need-customer" title="'+esc(g.customer)+'">'+esc(g.customer||'—')+'</strong><span class="ck-need-cargo" title="'+esc(g.cargo_summary||'见作业明细')+'">'+esc(g.cargo_summary||'见作业明细')+'</span><span class="ck-need-date" title="预计到货 '+esc(g.expected_arrival||'待定')+'">'+esc(g.expected_arrival||'待定')+'</span><span class="st st-'+esc(g.status)+'">'+esc(status)+'</span></div><div class="ck-need-brief"><span>'+g.count+'项作业 · 完成 '+g.completed+'项</span><span class="ck-need-summary" title="'+esc(summary)+'">'+esc(summary||'暂无有效作业')+'</span></div>';const open=btn('查看作业单 / 열기',()=>groupDetail({group_key:g.key}),'light ck-need-open');card.querySelector('.ck-need-main').append(open);c.append(card);}
 const pages=document.createElement('div');pages.className='toolbar';if(offset)pages.append(btn('上一页',()=>{offset=Math.max(0,offset-50);return load();},'light'));if(r.more)pages.append(btn('下一页',()=>{offset+=50;return load();},'light'));c.append(pages);
}
async function groupDetail(query){
 root.classList.remove('ck-needs-list');
 const r=await api('sop_need_groups',query),g=r.items[0];if(!g)throw Error('未找到本批作业指令');current=null;currentGroup=g.key;listSnapshot=JSON.stringify(r.items.map(g=>[g.key,g.items.map(x=>[x.id,x.revision])]));$('updates').textContent='';
 const c=$('content');c.innerHTML='<div class="toolbar" id="groupActions"></div><article class="card">'+groupHeader(g)+'<p class="muted">同一批货物的全部作业要求如下；分货依据以客服文字及箱唛范围为准。</p></article><article class="card"><div class="card-title">本批作业明细 <span class="count">'+g.count+'项 · 已完成 '+g.completed+'项</span></div><div class="scroll" id="groupTable">'+CKWorkNeedsTable(g.items)+'</div></article>';
 const actions=$('groupActions');actions.append(btn('← 返回作业指令列表',()=>load(),'light'));if(g.count){const korean=window.getLang?.()==='ko';const printButton=btn(korean?'이번 입고 작업 계획서 일괄 인쇄 ('+g.count+'건)':'批量打印本批作业单（'+g.count+'份）',()=>CKNeedBatchPrint(g),'light');printButton.dataset.ckBatchPrint=String(g.count);actions.append(printButton);const hint=document.createElement('span');hint.className='muted';hint.dataset.ckBatchPrintHint='';hint.textContent=korean?'인쇄 시 “PDF로 저장”을 선택하면 한 파일로 저장됩니다. 작업 계획서마다 새 페이지에서 시작하며 취소된 건은 제외합니다.':'打印时选择“保存为 PDF”，得到一个文件；每张作业单另起一页。已取消的作业单不打印。';actions.append(hint);}
 if(g.source_type==='inbound'&&g.source_id){if(options.context==='collab'&&window.openInboundDetail)actions.append(btn('查看入库计划',()=>openInboundDetail(g.source_id),'light'));else if(options.context!=='field'){const a=document.createElement('a');a.href='/002/?inbound='+encodeURIComponent(g.source_id);a.textContent='查看入库计划';actions.append(a);}}
 if(officeFiles&&g.source_type==='inbound'&&window.CKWorkChain?.enabled()){const files=document.createElement('section');c.querySelector('article').append(files);await CKWorkChain.batchMaterials(files,g);}
 const table=$('groupTable').querySelector('table');const th=document.createElement('th');th.textContent='操作';table.querySelector('thead tr').append(th);
 Array.from(table.querySelectorAll('tbody tr')).forEach((tr,i)=>{const td=document.createElement('td');td.append(btn(options.context==='field'?'派工／操作':'查看／处理',()=>detail(g.items[i].id,true),'light'));tr.append(td);});
}
 function needFields({source='inventory',doc={},supplement=false}={}){
  const section=(title,body)=>'<fieldset class="ck-plan-section"><legend>'+mark(title)+'</legend><div class="ck-plan-grid">'+body+'</div></fieldset>';
  const origin=source==='inventory'?'<input type="hidden" name="source_type" value="inventory"><p class="ck-plan-wide muted">'+mark('使用已有库存；入库作业请从入库计划建立，人员在现场派工时选择。')+'</p>':'<p class="ck-plan-wide muted">'+mark('关联来源')+'：'+esc(doc.display_no||doc.title||source)+'。'+mark('沿用原单据客户和来源，人员在现场派工时选择。')+'</p>';
  return section(source==='inventory'?'库存来源':'关联货物',origin+depInput()+input('customer','客户','text',doc.customer||'')+input('supply_chain_no',source==='inventory'?'供应链系统单号（必填）':'供应链系统单号（选填）','text',doc.supply_chain_no||'',source==='inventory')+input('scope_text','箱唛／货物范围（选填）','text','',false)+input('location','货物位置（选填）','text','',false))+
   section('作业要求与货量',input('title','作业名称','text',doc.title?'关联作业 '+doc.title:'')+select('operation_kind','作业类型 / 작업 종류',{operation:'需操作／加工',direct_forward:'直接转发（无加工）'})+'<div class="ck-plan-wide">'+area('instructions','操作要求',doc.instructions||'')+'</div>'+input('planned_quantity','本计划货量（已知则填）','number','',false)+select('planned_unit','货量单位',{'':'请选择单位',箱:'箱',件:'件',托:'托'})+'<p class="ck-plan-wide muted" data-quantity-help>'+mark('填写作业前的货量；未预约可暂不填。直接转发或同时预约出库时，数量和单位必填。')+'</p>'+input('deadline','要求完成时间（选填）','datetime-local','',false)+(supplement?input('reason','追加作业原因（必填）'):'') )+
   section('出库预约（可选）','<div id="optionalOutbounds" class="ck-plan-wide ck-work-plans"></div>');
 }
 function setupNeedFields(){
  const editor=$('editor'),quantity=editor.elements.planned_quantity,unit=editor.elements.planned_unit;
  quantity.min='1';quantity.step='1';
  const read=CKOptionalOutbounds($('optionalOutbounds'),{unit:()=>unit.value,unitControl:unit});
  const sync=()=>{const booking=!!$('optionalOutbounds').querySelector('[data-outbound-row]'),required=booking||editor.elements.operation_kind.value==='direct_forward';quantity.required=required;unit.required=required||!!quantity.value;};
  editor.oninput=sync;editor.onchange=sync;planObserver=new MutationObserver(sync);planObserver.observe($('optionalOutbounds'),{childList:true,subtree:true});sync();return read;
 }
 function create(kind){current=null;
  if(kind==='need'){let readObs;form('新增库存作业计划',needFields(),'sop_need_create',v=>({...v,outbounds:readObs()}),r=>detail(r.id));readObs=setupNeedFields();}
 if(kind==='task')taskForm();
 if(kind==='check')form('建立出库日期总清单',input('ship_date','出库日期','date'),'sop_check_create',v=>v,r=>detail(r.id));
 if(kind==='issue')form('接入现有问题',input('legacy_id','现有问题系统ID')+'<p class="warn">只接入没有正在进行处理轮次的问题。接入后统一在新版沟通，历史保留。</p>','sop_issue_adopt',v=>v,r=>detail(r.id));
}
function peopleFields(){return CKPeopleFields();}
function setupPeople(leadId='',workers=[]){staffPicker=CKPeoplePicker($('fields'),{workers,leadId,error:$('formError')});}
function taskForm(need){const types={bulk_op:'大货操作',unload:'卸货',load_outbound:'装货',inbound_bulk:'大货理货上架',inbound_direct:'代发入库',pick_direct:'代发拣货',pack_direct:'代发打包',inbound_return:'退件',qc:'质检',scan_pallet:'进口扫码码托',pickup_delivery_import:'取送货',load_import:'装柜',other_internal:'跨组支援／整理'};
 form('负责人创建任务','<p>关联作业计划在此派工；卸货、入库、拣货、装货等带原单据业务，请从现场首页对应入口派工，以完成单据状态回写。</p>'+depInput()+input('title','任务名称','text',need?.title||'')+select('job_type','作业类型',{bulk_op:types.bulk_op,other_internal:types.other_internal})+input('need_id','关联作业计划号','text',need?.display_no||'',false)+input('estimated_minutes','预计操作分钟','number','30')+input('deadline','预计完成时间','datetime-local','',false)+input('location','作业位置','text',need?.location||'',false)+peopleFields(),'sop_task_create',v=>({...v,need_id:need?.id||v.need_id,...staffPicker.read()}),r=>detail(r.id));setupPeople();if(need){$('editor').elements.department.value=need.department;$('editor').elements.need_id.readOnly=true;}}
async function detail(id,individual=!!window.CK_SOP_ROLLOUT?.workChain){const r=await api('sop_get',{id});if(r.record.kind==='need'&&!individual)return groupDetail({need_id:id});current=r.record;if(current.kind!=='need')currentGroup='';const x=current;$('updates').textContent='';const c=$('content');c.innerHTML=`<article><h2>${esc(x.title)} <span class="status">${mark(states[x.status]||x.status)}</span></h2><p>${mark(departments[x.department])} · ${esc(x.owner||'')} · ${esc(window.CKDocumentLabels.number(x))} · 版本 ${x.revision}</p><p>${esc(x.instructions||'')}</p><p>位置：${esc(x.location||'—')}　期限：${esc(x.deadline||'—')}</p><div class="actions" id="actions"></div></article><section id="detailBody"></section>`;
 const a=$('actions');a.append(btn(currentGroup?'← 返回本批作业指令':'返回列表',()=>currentGroup?groupDetail({group_key:currentGroup}):options.back?options.back():load(),'light'));
 const action=(label,action,html,build=v=>v)=>user.role==='viewer'?null:a.append(btn(label,()=>form(label,html,action,build)));
 if(x.kind==='need'){
  a.append(btn('打印作业单',()=>CKNeedPrint(x)));
  const info=document.createElement('p');info.textContent='货物范围：'+(x.scope_text||'见文字要求')+'　供应链单号：'+(x.supply_chain_no||'—');c.querySelector('article').append(info);
  if(x.status==='pending'){
   if(x.operation_kind!=='direct_forward'&&options.context==='field')a.append(btn('分配现场任务',()=>taskForm(x)));
   action('修改作业要求','sop_need_update',area('instructions','操作要求',x.instructions)+(window.CK_SOP_ROLLOUT?.workChain?input('planned_quantity','本作业计划数量','number',x.planned_quantity||'',false)+select('planned_unit','计划单位',{箱:'箱',件:'件',托:'托'},x.planned_unit):'')+input('owner','负责人','text',x.owner)+input('location','货物位置','text',x.location,false)+input('deadline','期限','datetime-local',x.deadline,false));
  }
  if(!window.CKWorkChain?.enabled()||x.operation_kind==='direct_forward')for(const link of (x.links||[]).filter(l=>l.phase==='planned'))action('调整预关联出库数量：'+link.quantity+link.unit,'sop_need_plan_quantity',input('quantity','调整后数量（'+link.unit+'）','number',link.quantity)+input('reason','调整原因'),v=>({...v,outbound_id:link.outbound_id}));
  if(x.task_id){const link=document.createElement('a');link.href='../001/?task='+encodeURIComponent(x.task_id);link.textContent='查看关联现场任务';a.append(link);}
  if(x.result){
   if(officeFiles){
   a.append(btn('下载打托明细模板 / 양식 다운로드',()=>CKDownloadWorkTemplate()));
   a.append(btn('上传打托明细 Excel',()=>{const file=document.createElement('input');file.type='file';file.accept='.xlsx,.xls';file.onchange=async()=>{try{if(await CKUploadWorkDetails(x,file.files[0]))await detail(x.id);}catch(e){notice(e.message);}};file.click();}));
   if(x.details){action('确认已转发客户','sop_need_forward',input('note','转发说明','text','',false));a.append(btn('下载当前明细',async()=>{const r=await api('v2_attachment_list',{related_doc_type:'sop_need',related_doc_id:x.id});const f=r.items.find(f=>f.id===x.details.attachment_id);if(!f)throw Error('原附件不存在');const a=document.createElement('a');a.href='/api?action=v2_attachment_get&file_key='+encodeURIComponent(f.file_key);a.download=f.file_name;a.target='_blank';a.click();}));}
   }
   $('detailBody').innerHTML=`<article><h3>审核通过的操作结果</h3>${CKResultSummary(x.result)}${x.result.location?`<p>原记录位置：${esc(x.result.location)}</p>`:''}${CKResultPhotos(x.result)}<p>已关联出库：${esc((x.links||[]).reduce((n,l)=>n+l.quantity,0))} ${esc(x.result.unit)}</p></article>`;
   if(officeFiles&&x.details){const p=document.createElement('p');p.textContent='打托明细 V'+x.details.version+'：'+x.details.filename+'，'+x.details.rows.length+'行，'+x.details.pallets+'托；'+(x.forwarded?'客服已转发客户':'待客服转发客户');$('detailBody').append(p);}
   if(['waiting_customer','linked'].includes(x.status)){const link=document.createElement('a');link.className='link';link.href='../002/?create_outbound=1&need='+encodeURIComponent(x.id);link.textContent='用此成果制定出库计划';if(!window.CK_SOP_ROLLOUT?.workChain)a.append(link);action(window.CK_SOP_ROLLOUT?.workChain?'关联历史出库计划':'关联出库计划','sop_need_link',input('outbound_id','出库计划系统ID')+input('quantity','本次关联数量（'+x.result.unit+'）','number'));action('登记后续去向并关闭','sop_need_close',area('reason','交接结果／剩余货物存放及责任人'));}}
 } else if(x.kind==='task'){
  $('detailBody').innerHTML=`<article><h3>实际参与人员</h3><p>${x.workers.map(w=>esc(w.name)+(w.id===x.lead_id?'〔主操作员〕':'')).join('、')}</p><p>预计 ${x.estimated_minutes} 分钟；作业轮次 ${x.round}</p>${x.pause_reason?`<p class="warn">暂停原因：${esc(x.pause_reason)}</p>`:''}${x.result?`<h3>完成结果</h3>${CKResultSummary(x.result)}${x.result.location?`<p>${esc(x.result.location)}</p>`:''}${CKResultPhotos(x.result)}`:''}${x.review?`<p>审核：${esc(x.review.by)} · ${esc(x.review.reason)}</p>`:''}</article>`;
  if(['assigned','paused','rework'].includes(x.status))action('开始／继续作业','sop_task_start','<p>确认人员已经到位，从提交成功开始记录工时。</p>');
  if(['assigned','working','paused','rework'].includes(x.status)){
   a.append(btn('调整参与人员',()=>{form('调整人员',peopleFields(x.workers)+input('reason','调整原因'),'sop_task_people',v=>({...v,...staffPicker.read()}));setupPeople(x.lead_id,x.workers);}));
   action('授权本任务代办','sop_task_delegate',input('delegate_id','已配置派工权限的人员ID'));
  }
  if(x.status==='working'){action('报告并暂停','sop_task_pause',area('reason','暂停原因'));action('操作完成，交审核','sop_task_finish',input('quantity','完成数量','number')+select('unit','单位',{件:'件',箱:'箱',托:'托',单:'单',批:'批'})+[['label_count','贴标数量'],['packed_count','打包数量'],['operated_box_count','操作箱数'],['pallet_count','打托数'],['used_carton_large_count','大纸箱用量'],['used_carton_small_count','小纸箱用量']].map(([key,label])=>input(key,label,'number','0')).join('')+area('description','实际完成明细（贴标／打包／耗材等）')+input('location','货物实际位置'),v=>({result:v}));}
  if(x.status==='awaiting_review'&&['manager','reviewer','dispatcher'].includes(user.role))action('审核／退回整改','sop_task_review',select('decision','结论',{pass:'审核通过',return:'退回整改'})+area('reason','审核检查结果或整改要求'));
  if(['assigned','paused','rework'].includes(x.status))action('作废任务','sop_task_cancel',area('reason','作废原因'));
 } else if(x.kind==='issue'){
  const changes=(x.changes||[]).map(m=>`<p class="warn">第 ${m.version} 版 · ${esc(m.by)}：${esc(m.text)}</p>`).join('');
  $('detailBody').innerHTML=`<article><h3>要求版本 ${x.requirement_version} / 已确认 ${x.ack_version}</h3>${changes}<h3>沟通与反馈</h3>${(x.messages||[]).map(m=>`<p><b>${esc(m.by)}</b> · ${esc(m.at)}<br>${esc(m.text)}</p>`).join('')}</article>`;
  if(x.status==='cancelled')return;
   if(x.status==='closed'){if(['manager','service'].includes(user.role))action('修改作业要求并重新开启','sop_issue_change',area('message','最新完整要求及重新开启原因',x.requirement_text||x.title));return;}
   if(['manager','service'].includes(user.role)){
    if(options.context!=='field')action('取消问题','sop_issue_cancel',area('reason','取消原因'));
    action('追加说明','sop_issue_append',area('message','追加内容'));
    action('修改作业要求','sop_issue_change',area('message','最新完整要求及修改原因（保留原记录）',x.requirement_text||x.title));
    if(x.status==='responded')action('确认关闭','sop_issue_close','<p>确认仓库已完成最新要求。</p>');
   }
   if(['manager','dispatcher','reviewer'].includes(user.role)){
    if(x.requirement_version>x.ack_version)action('确认已阅读最新要求','sop_issue_ack','<p>确认当前页面展示的最新要求；提交后如再次变更，仍需重新确认。</p>',()=>({requirement_version:x.requirement_version}));
    action('仓库反馈','sop_issue_feedback',area('message','处理结果'));
   }
 } else if(x.kind==='check')renderCheck(x,a,action);
 if(officeFiles&&x.kind==='need'&&window.CKWorkChain?.enabled()){const host=document.createElement('section');$('detailBody').append(host);await CKWorkChain.mountNeed(host,x,{onChange:()=>detail(x.id,true)});}
 const history=document.createElement('details');history.className='card';history.innerHTML=`<summary>操作历史（最近100次，含修改前后）</summary>${r.events.map(e=>`<p><b>${esc(e.actor_name)}</b> · ${esc(e.created_at)} · ${esc(e.action)}</p><details><summary>查看修改前后</summary><pre>${esc(e.before_json)}\n→\n${esc(e.after_json)}</pre></details>`).join('')}`;c.append(history);
}
function renderCheck(x,a,action){const s=x.summary;$('detailBody').innerHTML=`<article><p>原计划 ${s.original}　追加 ${s.added}　调整 ${s.adjusted}　撤销 ${s.removed}　当前有效 ${s.planned}　已核对 ${s.scanned}　待核对条目 ${s.pending}　未处理异常 ${s.unresolved}</p><div class="scroll"><table><thead><tr><th>条码</th><th><span data-i18n="ck_plan_17">客户</span></th><th>计划箱数</th><th>已核对</th><th>状态</th><th>操作</th></tr></thead><tbody id="items"></tbody></table></div></article><article><h3>核对轮次</h3>${x.rounds.map(r=>`<p>${esc(r.slot)} · ${esc(r.by)} · ${esc(r.at)} · ${esc(r.note)}</p>`).join('')||'暂无已保存轮次'}</article><article id="exceptions"><h3>扫描异常</h3></article>`;
 for(const it of x.items){const tr=document.createElement('tr');tr.innerHTML=`<td>${esc(it.barcode)}</td><td>${esc(it.customer)}</td><td>${it.qty}</td><td>${it.scanned}</td><td>${it.removed?'已撤销':it.recheck?'待复核':it.scanned===it.qty?'核对一致':'待核对'}</td><td></td>`;if(!it.removed&&x.status==='open')tr.lastChild.append(btn('撤销',()=>form('撤销条目',area('reason','撤销原因')+area('disposition','已核对货物实际去向','',!!(it.scanned||it.previous_scanned)),'sop_check_remove',v=>({...v,item_id:it.id})),'light'));if(!it.removed&&x.status==='open')tr.lastChild.append(btn('改出库日期',()=>form('调整出库日期',input('ship_date','目标日期','date')+area('reason','改期原因')+area('disposition','实物去向'),'sop_check_move',v=>({...v,item_id:it.id})),'light'));$('items').append(tr);}
 for(const ex of x.exceptions){const p=document.createElement('p');p.textContent=ex.barcode+' · '+(ex.type==='overflow'?'超出计划':'不在清单')+' · '+(ex.resolved?'已复核':'未处理');if(!ex.resolved)p.append(btn('处理并复核',()=>form('异常复核',area('reason','实物处理及复核结果'),'sop_check_resolve',v=>({...v,exception_id:ex.id}))));$('exceptions').append(p);}
 if(x.status==='closed'){action('负责人重新开启','sop_check_reopen',area('reason','重新开启原因'));return;}
 a.append(btn('新增／修改条码',()=>{form('追加或修改清单','<p>每行：条码,客户,计划箱数。条码列须以文本保存，保留前导零。</p><label>导入Excel / CSV<input type="file" id="import" accept=".xlsx,.xls,.csv"></label>'+area('rows','条码,客户,计划箱数')+select('duplicate_mode','已存在条码处理',{error:'提示冲突，不提交',skip:'跳过',replace:'修改原数量并要求重新核对'})+input('reason','修改原因','text','',false),'sop_check_items',v=>({...v,items:v.rows.split(/\r?\n/).filter(Boolean).map(line=>{const [barcode,customer,qty]=line.split(/[,\t]/);return{barcode,customer,qty:Number(qty)};})}));$('import').onchange=async e=>{try{const f=e.target.files[0],wb=XLSX.read(await f.arrayBuffer(),{type:'array'}),rows=XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]],{header:1,raw:false});if(rows[0]?.[0]?.includes('条码'))rows.shift();$('editor').elements.rows.value=rows.filter(r=>r[0]).map(r=>r.slice(0,3).join('\t')).join('\n');}catch(e){$('formError').textContent=e.message;}};}));
 a.append(btn('扫描核对',()=>scanForm(x,'')));
 action('保存本轮核对','sop_check_round',select('slot','轮次',{'11:00':'11:00','13:00':'13:00','16:00':'16:00',追加:'紧急追加'})+area('reason','本轮交接及未完成事项'));
 action('最终交接并关闭','sop_check_close',area('reason','接收人、交接位置及结果'));
}
let dashboardDate='',rankFilter='';
async function renderDashboard(){const date=dashboardDate||new Date(Date.now()+9*3600000).toISOString().slice(0,10),r=await api('sop_dashboard',{date});$('content').innerHTML=`<article><h2>人员作业与每日产量 · ${r.date}</h2><label>统计日期<input id="dashboardDate" type="date" value="${esc(r.date)}"></label><label>查看视角<select id="audience"><option value="manager">经理调度</option><option value="boss">老板总览</option><option value="leader">组长执行</option></select></label><p class="warn">${esc(r.scope)}</p><div class="grid">${[['正在作业人数',r.active_people],['进行中任务',r.working],['审核通过任务',r.completed],['实际人时',r.person_hours]].map(([k,v])=>`<div><span>${k}</span><div class="metric">${v}</div></div>`).join('')}</div></article><article><h3>合格成果（分作业、分单位）</h3>${Object.entries(r.outputs).map(([k,v])=>`<p>${esc(k)}：${v}</p>`).join('')||'暂无审核通过成果'}</article><article><h3>数据待核实</h3>${r.data_quality.map(x=>`<p>${esc(x.title)}：${esc(x.reason)}</p>`).join('')||'当前没有此类提醒'}</article><article id="alerts"><h3>待办与异常</h3></article>`;
 $('dashboardDate').onchange=e=>{dashboardDate=e.target.value;renderDashboard().catch(e=>notice(e.message));};
 const board=document.createElement('article');board.innerHTML='<h3>人员当日排名（按相同业务、作业及单位比较）</h3><label>选择排名项目<select id="rankFilter"></select></label><div id="rankRows"></div><p>现场已报完成量含待审核；排名中的关联作业只计审核通过，原业务任务按原流程完成记录计入。</p>';$('content').append(board);
 const groups=[...new Set((r.rankings||[]).map(x=>[x.department,x.job_type,x.metric,x.unit].join(' / ')))];$('rankFilter').innerHTML=groups.map(k=>'<option>'+esc(k)+'</option>').join('');if(groups.includes(rankFilter))$('rankFilter').value=rankFilter;
 const paint=()=>{rankFilter=$('rankFilter').value;const rows=(r.rankings||[]).filter(x=>[x.department,x.job_type,x.metric,x.unit].join(' / ')===rankFilter);$('rankRows').innerHTML=rows.length?'<table><thead><tr><th>名次</th><th>人员</th><th>分配产量</th><th>完成任务数</th></tr></thead><tbody>'+rows.map((x,i)=>'<tr><td>'+(i&&x.quantity===rows[i-1].quantity?rows.findIndex(y=>y.quantity===x.quantity)+1:i+1)+'</td><td>'+esc(x.worker_name)+'</td><td>'+Number(x.quantity.toFixed(2))+' '+esc(x.unit)+'</td><td>'+x.tasks.length+'</td></tr>').join('')+'</tbody></table>':'暂无可排名成果';};$('rankFilter').onchange=paint;paint();
 const completed=document.createElement('article');completed.innerHTML='<h3>整单完成后已报产量</h3>'+((r.reported_outputs||[]).map(x=>'<p>'+esc(x.worker_name)+' · '+esc(x.metric)+' '+Number(x.quantity.toFixed(2))+esc(x.unit)+' · '+esc(x.verification)+'</p>').join('')||'暂无完成报告');$('content').append(completed);
 const live=document.createElement('article');live.id='livePeople';live.innerHTML='<h3>当前作业人员</h3>'+(r.roster||[]).map(x=>'<p>'+esc(x.worker_name)+' · '+esc(x.status)+' · '+esc(x.current_task)+' · '+esc(x.business_no||'')+'</p>').join('');$('content').append(live);$('audience').onchange=e=>{live.hidden=e.target.value==='boss';};
 for(const x of r.alerts){const p=document.createElement('p');p.textContent=(departments[x.department]||'')+' · '+x.title+' · '+(states[x.status]||x.status)+' · '+x.owner+' ';p.append(btn('查看',()=>detail(x.id),'light'));$('alerts').append(p);}
 $('content').append(btn('导出今日待办日报 CSV',()=>{const rows=[['日期','部门','任务号','内容','状态','负责人'],...r.alerts.map(x=>[r.date,departments[x.department],x.work_plan_no||x.display_no||'单号待补充',x.title,states[x.status],x.owner])];const csv='\ufeff'+rows.map(row=>row.map(v=>'"'+String(v??'').replace(/^[=+@-]/,"'$&").replace(/"/g,'""')+'"').join(',')).join('\r\n');const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='CK执行日报-'+r.date+'.csv';a.click();URL.revokeObjectURL(url);}));
}

async function attachSources(name,type){
 const input=$('editor').elements[name];if(!input)return;
 const r=await api('sop_sources',{type,department:$('editor').elements.department?.value||'bulk'});
 const list=document.createElement('datalist');list.id='ck-sources-'+name;root.querySelector('[id="'+list.id+'"]')?.remove();list.innerHTML=r.items.map(x=>'<option value="'+esc(x.number||x.label.split(' / ')[0])+'">'+esc(x.label)+'</option>').join('');input._ckSources=r.items;input.setAttribute('list',list.id);input.after(list);
}
async function openSource(){
 if(options.source==='outbound'&&window.CK_SOP_ROLLOUT?.workChain)throw Error('请先从入库计划或库内库存建立作业计划，再安排出库');
 const source=entry.get('source'),source_id=entry.get('source_id');
 if(!source||!source_id)return load();
 if(source==='issue'){
  try{return await detail('ISSUE-'+source_id);}catch{}
  await load();create('issue');$('editor').elements.legacy_id.value=source_id;return;
 }
 if(source==='check'){
  await load();form('将旧核对批次接入日期总清单',input('ship_date','出库日期','date')+input('legacy_id','原批次ID','text',source_id)+'<p>保留原扫描记录。旧批次有进行中任务时禁止接入。</p>','sop_check_adopt',v=>v,r=>detail(r.id));return;
 }
 const linked=await api('sop_linked',{source_id});
 if(linked.items.length&&!options.supplement){await load();notice('此单已有作业计划，请引用已有记录。');for(const x of linked.items)$('content').prepend(btn('打开关联作业：'+x.title,()=>detail(x.id)));return;}
 const r=await api('sop_source_detail',{source_id,type:source});const doc=r.source;
 await load();
  let readObs;form(source==='outbound'?'将出库操作统一为关联作业':'从入库计划创建作业计划',needFields({source,doc,supplement:options.supplement}),source==='outbound'?'sop_need_from_outbound':'sop_need_create',v=>({...v,source_id,source_type:source,outbounds:readObs()}),r=>detail(r.id));readObs=setupNeedFields();
  $('editor').elements.department.value=doc.department;
  $('editor').elements.customer.readOnly=true;
}
function scanForm(record,pallet){
 current=record;
 form('扫码核对',input('pallet','托盘号／散箱位置','text',pallet)+input('barcode','条码（每箱扫描一次）'),'sop_check_scan',v=>v,async r=>{const latest=await api('sop_get',{id:r.id});await detail(r.id);scanForm(latest.record,pallet||'');});
 const f=$('editor'),original=f.onsubmit;f.onsubmit=e=>{pallet=f.elements.pallet.value;return original(e);};f.elements.barcode.focus();
}

let destroyed=false;
const start=async()=>{if(destroyed)return;try{if(options.group_source_id)await groupDetail({source_type:'inbound',source_id:options.group_source_id});else if(options.id)await detail(options.id,!!options.individual);else {await openSource();if(options.create)create(options.create);}}catch(e){notice(e.message);}if(!destroyed)poll=setInterval(checkUpdates,20000);};
start();
return {load,detail,create,taskForm,destroy(){destroyed=true;clearInterval(poll);closeModal();root.replaceChildren();}};
};
})();
