(function(){
'use strict';
window.CKWorkflow=function(root,options={}){
root.classList.add('ck-workflow');
root.innerHTML=`<div id="notice" role="status" aria-live="polite"></div><div class="toolbar"><button id="refresh">刷新 / 새로고침</button><span id="updates"></span></div><section id="content"></section><dialog id="modal"><form id="editor"><h2 id="editorTitle"></h2><div id="fields"></div><div class="toolbar"><button type="submit" id="save">保存 / 저장</button><button type="button" id="cancel" class="light">关闭 / 닫기</button></div><p id="formError" role="alert"></p></form></dialog>`;
const $=id=>root.querySelector('[id="'+id+'"]'), esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const entry=new URLSearchParams(options);
const labels={need:'作业需求 / 작업 요청',task:'负责人派工 / 작업 배정',issue:'问题沟通 / 이슈',check:'出库核对 / 출고 확인',dashboard:'管理看板 / 관리 현황'};
const departments={bulk:'大货 / 대량',direct_ship:'代发 / 출고대행',import:'进口 / 수입'};
const states={pending:'待安排',assigned:'已分配',working:'作业中',paused:'已暂停',awaiting_review:'待审核',rework:'待整改',completed:'审核通过',waiting_customer:'待客户安排',linked:'已关联出库',closed:'已关闭',cancelled:'已作废',open:'处理中',responded:'已反馈'};
let user=window.CKSession?.user,tab=Object.hasOwn(labels,entry.get('tab'))?entry.get('tab'):'need',offset=0,current=null,scanner=null,people=[],poll=null,listSnapshot='';
async function api(action,data={}){return window.CKSession.request(action,data);}
function notice(s){$('notice').textContent=s;}
function btn(label,fn,cls=''){const b=document.createElement('button');b.textContent=label;b.className=cls;b.type='button';b.onclick=()=>Promise.resolve().then(fn).catch(e=>notice(e.message));return b;}
function input(name,label,type='text',value='',required=true){return `<label>${esc(label)}<input name="${name}" type="${type}" value="${esc(value)}" ${required?'required':''}></label>`;}
function area(name,label,value='',required=true){return `<label>${esc(label)}<textarea name="${name}" ${required?'required':''}>${esc(value)}</textarea></label>`;}
function select(name,label,values,value=''){return `<label>${esc(label)}<select name="${name}">${Object.entries(values).map(([k,v])=>`<option value="${esc(k)}" ${k===value?'selected':''}>${esc(v)}</option>`).join('')}</select></label>`;}
function depInput(){const ds=user.role==='manager'?departments:Object.fromEntries((user.departments||[]).map(k=>[k,departments[k]||k]));return select('department','部门 / 부서',ds);}
async function closeModal(){if(scanner){await scanner.stop().catch(()=>{});scanner=null;}$('modal').close();}
// Retry the identical request after a transport error. Never create a second mutation id.
function form(title,html,action,build,after){
 $('editorTitle').textContent=title;$('fields').innerHTML=html;$('formError').textContent='';$('save').disabled=false;
 let pending=null;const revision=current?.revision,id=current?.id;
 $('editor').onsubmit=async e=>{e.preventDefault();$('save').disabled=true;
 try{if(!pending){const values=Object.fromEntries(new FormData($('editor')));pending={...await build(values),client_req_id:crypto.randomUUID()};if(id&&!pending.id&& !action.endsWith('_create')&&action!=='sop_issue_adopt'){pending.id=id;pending.revision=revision;}}
  const result=await api(action,pending);await closeModal();await(after?after(result):id?detail(id):load());notice('已保存 / 저장 완료');
 }catch(e){$('formError').textContent=e.message; // Business errors return a response: allow corrections with a new request.
  if(!(e instanceof TypeError)){pending=null;}
 }finally{$('save').disabled=false;}};
 $('modal').showModal();
}
$('cancel').onclick=closeModal;
$('modal').addEventListener('cancel',e=>{e.preventDefault();closeModal();});
$('refresh').onclick=()=> (current?detail(current.id):load()).catch(e=>notice(e.message));
function renderTabs(){}
async function checkUpdates(){if(tab==='dashboard'&&user&&!document.hidden&&root.isConnected&&root.offsetParent!==null){await renderDashboard();return;}if(!user||document.hidden||!root.isConnected||root.offsetParent===null||$('modal').open)return;try{if(current){const r=await api('sop_get',{id:current.id});if(r.record.revision!==current.revision)$('updates').textContent='有新要求/新记录，请刷新查看并确认 · 새 변경사항';}else if(tab!=='dashboard'){const r=await api('sop_list',{kind:tab,offset});if(JSON.stringify(r.items.map(x=>[x.id,x.revision]))!==listSnapshot)$('updates').textContent='列表有更新，请刷新 · 업데이트';}}catch{} }
async function load(){notice('');$('updates').textContent='';current=null;const c=$('content');c.innerHTML='<p>加载中…</p>';if(tab==='dashboard')return renderDashboard();
 const r=await api('sop_list',{kind:tab,offset});listSnapshot=JSON.stringify(r.items.map(x=>[x.id,x.revision]));c.innerHTML='';const bar=document.createElement('div');bar.className='toolbar';
 const createLabels={need:'新建作业需求',task:'新建派工任务',check:'按出库日期建清单',issue:'接入现有问题'};
 if(user.role!=='viewer')bar.append(btn(createLabels[tab],()=>create(tab)));
 if(offset)bar.append(btn('上一页',()=>{offset=Math.max(0,offset-50);return load();},'light'));
 if(r.more)bar.append(btn('下一页',()=>{offset+=50;return load();},'light'));c.append(bar);
 if(!r.items.length)c.insertAdjacentHTML('beforeend','<div class="card muted">暂无记录。请从关联单据建立作业，或点击上方按钮。</div>');
 for(const x of r.items){const a=document.createElement('article');a.innerHTML=`<h3>${esc(x.title)} <span class="status">${esc(states[x.status]||x.status)}</span></h3><p>${esc(departments[x.department])} · ${esc(x.owner||'')} · ${esc(x.location||'')}</p><p class="muted">${esc(x.id)} · 版本 ${x.revision}</p>${x.kind==='issue'&&x.requirement_version>x.ack_version?'<p class="warn">作业要求有变更，仓库待确认</p>':''}`;a.append(btn('打开 / 열기',()=>detail(x.id)));c.append(a);}
}
function create(kind){current=null;
 if(kind==='need'){form('新建作业需求',depInput()+input('title','作业名称')+input('customer','客户')+select('source_type','关联来源',{inventory:'库内库存',inbound:'入库计划',outbound:'出库计划'})+input('supply_chain_no','供应链系统单号（库内库存必填）','text','',false)+input('scope_text','箱唛／货物范围','text','',false)+input('planned_quantity','本作业计划数量（同步出库必填）','number','',false)+select('planned_unit','计划单位',{箱:'箱',件:'件',托:'托'})+input('source_id','关联单据（可下拉选择）','text','',false)+area('instructions','操作要求')+input('owner','接单负责人')+input('deadline','要求完成时间','datetime-local','',false)+input('location','货物位置','text','',false)+input('reason','已有需求时的追加原因','text','',false)+'<div id=optionalOutbounds></div>','sop_need_create',v=>({...v,outbounds:readObs()}),r=>detail(r.id));const readObs=CKOptionalOutbounds($('optionalOutbounds'));$('editor').elements.source_type.onchange=e=>{if(e.target.value!=='inventory')attachSources('source_id',e.target.value);else $('editor').elements.source_id.value='';};}
 if(kind==='task')taskForm();
 if(kind==='check')form('建立出库日期总清单',input('ship_date','出库日期','date'),'sop_check_create',v=>v,r=>detail(r.id));
 if(kind==='issue')form('接入现有问题',input('legacy_id','现有问题系统ID')+'<p class="warn">只接入没有正在进行处理轮次的问题。接入后统一在新版沟通，历史保留。</p>','sop_issue_adopt',v=>v,r=>detail(r.id));
}
function peopleFields(existing=[]){people=existing.slice();return `<label>扫描工牌 / 명찰<input id="badge" placeholder="EMP-001|姓名"></label><div class="toolbar"><button type="button" id="addBadge">添加工牌</button><button type="button" id="camera">打开相机 / 카메라</button></div><div id="scanner"></div><div id="people"></div><label>主操作员<select name="lead_id" id="lead" required></select></label>`;}
function setupPeople(lead){const render=()=>{$('people').innerHTML='';for(const w of people){const s=document.createElement('span');s.className='badge';s.textContent=w.name+' ('+w.id+') ';const b=btn('移除',()=>{people=people.filter(x=>x.id!==w.id);render();},'light');b.type='button';s.append(b);$('people').append(s);}$('lead').innerHTML=people.map(w=>`<option value="${esc(w.id)}" ${w.id===lead?'selected':''}>${esc(w.name)}</option>`).join('');};
 const add=raw=>{const parts=raw.trim().split('|');if(parts.length!==2||!parts[0]||!parts[1])throw Error('工牌格式应为 工号|姓名');if(people.some(x=>x.id===parts[0]))return;people.push({id:parts[0],name:parts[1]});render();$('badge').value='';};
 $('addBadge').onclick=()=>{try{add($('badge').value);}catch(e){$('formError').textContent=e.message;}};
 $('badge').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();$('addBadge').click();}};
 $('camera').onclick=async()=>{try{if(scanner){await scanner.stop();scanner=null;return;}scanner=new Html5Qrcode('scanner');await scanner.start({facingMode:'environment'},{fps:8,qrbox:220},s=>{try{add(s);}catch(e){$('formError').textContent=e.message;}},()=>{});}catch(e){$('formError').textContent='无法打开相机，可用扫码枪或手动输入工牌';scanner=null;}};render();}
function taskForm(need){const types={bulk_op:'大货操作',unload:'卸货',load_outbound:'装货',inbound_bulk:'大货理货上架',inbound_direct:'代发入库',pick_direct:'代发拣货',pack_direct:'代发打包',inbound_return:'退件',qc:'质检',scan_pallet:'进口扫码码托',pickup_delivery_import:'取送货',load_import:'装柜',other_internal:'跨组支援／整理'};
 form('负责人创建任务','<p>关联作业需求在此派工；卸货、入库、拣货、装货等带原单据业务，请从现场首页对应入口派工，以完成单据状态回写。</p>'+depInput()+input('title','任务名称','text',need?.title||'')+select('job_type','作业类型',{bulk_op:types.bulk_op,other_internal:types.other_internal})+input('need_id','关联作业需求','text',need?.id||'',false)+input('estimated_minutes','预计操作分钟','number','30')+input('deadline','预计完成时间','datetime-local','',false)+input('location','作业位置','text',need?.location||'',false)+peopleFields(),'sop_task_create',v=>({...v,workers:people}),r=>detail(r.id));setupPeople();if(need){$('editor').elements.department.value=need.department;$('editor').elements.need_id.readOnly=true;}}
async function detail(id){const r=await api('sop_get',{id});current=r.record;const x=current;$('updates').textContent='';const c=$('content');c.innerHTML=`<article><h2>${esc(x.title)} <span class="status">${esc(states[x.status]||x.status)}</span></h2><p>${esc(departments[x.department])} · ${esc(x.owner||'')} · ${esc(x.id)} · 版本 ${x.revision}</p><p>${esc(x.instructions||'')}</p><p>位置：${esc(x.location||'—')}　期限：${esc(x.deadline||'—')}</p><div class="actions" id="actions"></div></article><section id="detailBody"></section>`;
 const a=$('actions');a.append(btn('返回列表',()=>options.back?options.back():load(),'light'));
 const action=(label,action,html,build=v=>v)=>user.role==='viewer'?null:a.append(btn(label,()=>form(label,html,action,build)));
 if(x.kind==='need'){
  a.append(btn('打印作业单',()=>CKNeedPrint(x)));
  const info=document.createElement('p');info.textContent='货物范围：'+(x.scope_text||'见文字要求')+'　供应链单号：'+(x.supply_chain_no||'—');c.querySelector('article').append(info);
  if(x.status==='pending'){
   if(options.context==='field')a.append(btn('分配现场任务',()=>taskForm(x)));
   else {const link=document.createElement('a');link.className='link';link.href='../001/?need='+encodeURIComponent(x.id);link.textContent='交现场负责人派工 / 현장 배정';a.append(link);}
   action('修改作业要求','sop_need_update',area('instructions','操作要求',x.instructions)+input('owner','负责人','text',x.owner)+input('location','货物位置','text',x.location,false)+input('deadline','期限','datetime-local',x.deadline,false));
  }
  for(const link of (x.links||[]).filter(l=>l.phase==='planned'))action('调整预关联出库数量：'+link.quantity+link.unit,'sop_need_plan_quantity',input('quantity','调整后数量','number',link.quantity)+input('reason','调整原因'),v=>({...v,outbound_id:link.outbound_id}));
  if(x.task_id){const link=document.createElement('a');link.href='../001/?task='+encodeURIComponent(x.task_id);link.textContent='查看关联现场任务';a.append(link);}
  if(x.result){
   a.append(btn('上传打托明细 Excel',()=>{const file=document.createElement('input');file.type='file';file.accept='.xlsx,.xls';file.onchange=async()=>{try{if(await CKUploadWorkDetails(x,file.files[0]))await detail(x.id);}catch(e){notice(e.message);}};file.click();}));
   if(x.details){action('确认已转发客户','sop_need_forward',input('note','转发说明','text','',false));a.append(btn('下载当前明细',async()=>{const r=await api('v2_attachment_list',{related_doc_type:'sop_need',related_doc_id:x.id});const f=r.items.find(f=>f.id===x.details.attachment_id);if(!f)throw Error('原附件不存在');const a=document.createElement('a');a.href='/api?action=v2_attachment_get&file_key='+encodeURIComponent(f.file_key);a.download=f.file_name;a.target='_blank';a.click();}));}
   $('detailBody').innerHTML=`<article><h3>审核通过的操作结果</h3><p>${esc(x.result.quantity)} ${esc(x.result.unit)} · ${esc(x.result.description)}</p><p>位置：${esc(x.result.location)}</p><p>已关联出库：${esc((x.links||[]).reduce((n,l)=>n+l.quantity,0))} ${esc(x.result.unit)}</p></article>`;
   if(x.details){const p=document.createElement('p');p.textContent='打托明细 V'+x.details.version+'：'+x.details.filename+'，'+x.details.rows.length+'行，'+x.details.pallets+'托；'+(x.forwarded?'客服已转发客户':'待客服转发客户');$('detailBody').append(p);}
   if(['waiting_customer','linked'].includes(x.status)){const link=document.createElement('a');link.className='link';link.href='../002/?create_outbound=1&need='+encodeURIComponent(x.id);link.textContent='用此成果制定出库计划';a.append(link);action('关联出库计划','sop_need_link',input('outbound_id','出库计划系统ID')+input('quantity','本次关联数量（'+x.result.unit+'）','number'));action('登记后续去向并关闭','sop_need_close',area('reason','交接结果／剩余货物存放及责任人'));}}
 } else if(x.kind==='task'){
  $('detailBody').innerHTML=`<article><h3>实际参与人员</h3><p>${x.workers.map(w=>esc(w.name)+(w.id===x.lead_id?'〔主操作员〕':'')).join('、')}</p><p>预计 ${x.estimated_minutes} 分钟；作业轮次 ${x.round}</p>${x.pause_reason?`<p class="warn">暂停原因：${esc(x.pause_reason)}</p>`:''}${x.result?`<h3>完成结果</h3><p>${esc(x.result.quantity)} ${esc(x.result.unit)} · ${esc(x.result.description)}</p><p>${esc(x.result.location)}</p>`:''}${x.review?`<p>审核：${esc(x.review.by)} · ${esc(x.review.reason)}</p>`:''}</article>`;
  if(['assigned','paused','rework'].includes(x.status))action('开始／继续作业','sop_task_start','<p>确认人员已经到位，从提交成功开始记录工时。</p>');
  if(['assigned','working','paused','rework'].includes(x.status)){
   a.append(btn('调整参与人员',()=>{form('调整人员',peopleFields(x.workers)+input('reason','调整原因'),'sop_task_people',v=>({...v,workers:people}));setupPeople(x.lead_id);}));
   action('授权本任务代办','sop_task_delegate',input('delegate_id','已配置派工权限的人员ID'));
  }
  if(x.status==='working'){action('报告并暂停','sop_task_pause',area('reason','暂停原因'));action('操作完成，交审核','sop_task_finish',input('quantity','完成数量','number')+select('unit','单位',{件:'件',箱:'箱',托:'托',单:'单',批:'批'})+[['label_count','贴标数量'],['packed_count','打包数量'],['operated_box_count','操作箱数'],['pallet_count','打托数'],['used_carton_large_count','大纸箱用量'],['used_carton_small_count','小纸箱用量']].map(([key,label])=>input(key,label,'number','0')).join('')+area('description','实际完成明细（贴标／打包／耗材等）')+input('location','货物实际位置'),v=>({result:v}));}
  if(x.status==='awaiting_review'&&['manager','reviewer','dispatcher'].includes(user.role))action('审核／退回整改','sop_task_review',select('decision','结论',{pass:'审核通过',return:'退回整改'})+area('reason','审核检查结果或整改要求'));
  if(['assigned','paused','rework'].includes(x.status))action('作废任务','sop_task_cancel',area('reason','作废原因'));
 } else if(x.kind==='issue'){
  const changes=(x.changes||[]).map(m=>`<p class="warn">第 ${m.version} 版 · ${esc(m.by)}：${esc(m.text)}</p>`).join('');
  $('detailBody').innerHTML=`<article><h3>要求版本 ${x.requirement_version} / 已确认 ${x.ack_version}</h3>${changes}<h3>沟通与反馈</h3>${(x.messages||[]).map(m=>`<p><b>${esc(m.by)}</b> · ${esc(m.at)}<br>${esc(m.text)}</p>`).join('')}</article>`;
  if(x.status==='cancelled')return;
  if(options.context!=='field'){action('取消问题','sop_issue_cancel',area('reason','取消原因'));}
  action('追加说明','sop_issue_append',area('message','追加内容'));
  action('修改作业要求','sop_issue_change',area('message','最新完整要求及修改原因（保留原记录）',x.requirement_text||x.title));
  if(x.requirement_version>x.ack_version)action('确认已阅读最新要求','sop_issue_ack','<p>确认当前页面展示的最新要求；提交后如再次变更，仍需重新确认。</p>',()=>({requirement_version:x.requirement_version}));
  action('仓库反馈','sop_issue_feedback',area('message','处理结果'));
  if(x.status==='responded')action('确认关闭','sop_issue_close','<p>确认仓库已完成最新要求。</p>');
 } else if(x.kind==='check')renderCheck(x,a,action);
 const history=document.createElement('details');history.className='card';history.innerHTML=`<summary>操作历史（最近100次，含修改前后）</summary>${r.events.map(e=>`<p><b>${esc(e.actor_name)}</b> · ${esc(e.created_at)} · ${esc(e.action)}</p><details><summary>查看修改前后</summary><pre>${esc(e.before_json)}\n→\n${esc(e.after_json)}</pre></details>`).join('')}`;c.append(history);
}
function renderCheck(x,a,action){const s=x.summary;$('detailBody').innerHTML=`<article><p>原计划 ${s.original}　追加 ${s.added}　调整 ${s.adjusted}　撤销 ${s.removed}　当前有效 ${s.planned}　已核对 ${s.scanned}　待核对条目 ${s.pending}　未处理异常 ${s.unresolved}</p><div class="scroll"><table><thead><tr><th>条码</th><th>客户</th><th>计划箱数</th><th>已核对</th><th>状态</th><th>操作</th></tr></thead><tbody id="items"></tbody></table></div></article><article><h3>核对轮次</h3>${x.rounds.map(r=>`<p>${esc(r.slot)} · ${esc(r.by)} · ${esc(r.at)} · ${esc(r.note)}</p>`).join('')||'暂无已保存轮次'}</article><article id="exceptions"><h3>扫描异常</h3></article>`;
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
 const live=document.createElement('article');live.id='livePeople';live.innerHTML='<h3>当前作业人员</h3>'+(r.roster||[]).map(x=>'<p>'+esc(x.worker_name)+' · '+esc(x.status)+' · '+esc(x.current_task)+' · '+esc(x.current_job_id)+'</p>').join('');$('content').append(live);$('audience').onchange=e=>{live.hidden=e.target.value==='boss';};
 for(const x of r.alerts){const p=document.createElement('p');p.textContent=(departments[x.department]||'')+' · '+x.title+' · '+(states[x.status]||x.status)+' · '+x.owner+' ';p.append(btn('查看',()=>detail(x.id),'light'));$('alerts').append(p);}
 $('content').append(btn('导出今日待办日报 CSV',()=>{const rows=[['日期','部门','任务号','内容','状态','负责人'],...r.alerts.map(x=>[r.date,departments[x.department],x.id,x.title,states[x.status],x.owner])];const csv='\ufeff'+rows.map(row=>row.map(v=>'"'+String(v??'').replace(/^[=+@-]/,"'$&").replace(/"/g,'""')+'"').join(',')).join('\r\n');const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='CK执行日报-'+r.date+'.csv';a.click();URL.revokeObjectURL(url);}));
}

async function attachSources(name,type){
 const input=$('editor').elements[name];if(!input)return;
 const r=await api('sop_sources',{type,department:$('editor').elements.department?.value||'bulk'});
 const list=document.createElement('datalist');list.id='ck-sources-'+name;root.querySelector('[id="'+list.id+'"]')?.remove();list.innerHTML=r.items.map(x=>'<option value="'+esc(x.id)+'">'+esc(x.label)+'</option>').join('');input.setAttribute('list',list.id);input.after(list);
}
async function openSource(){
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
 if(linked.items.length&&!options.supplement){await load();notice('此单已有作业需求，请引用已有记录。');for(const x of linked.items)$('content').prepend(btn('打开关联作业：'+x.title,()=>detail(x.id)));return;}
 const r=await api('sop_source_detail',{source_id,type:source});const doc=r.source;
 await load();
 form(source==='outbound'?'将出库操作统一为关联作业':'从入库计划创建作业需求',depInput()+input('title','作业名称','text','关联作业 '+doc.title)+input('customer','客户','text',doc.customer)+area('instructions','操作要求',doc.instructions)+input('owner','接单负责人')+input('scope_text','箱唛／货物范围','text','',false)+input('supply_chain_no','供应链系统单号','text','',false)+input('planned_quantity','计划数量（同步出库必填）','number','',false)+select('planned_unit','计划单位',{箱:'箱',件:'件',托:'托'})+'<div id=optionalOutbounds></div>'+input('location','货物位置','text','',false)+input('deadline','完成期限','datetime-local','',false)+(options.supplement?input('reason','新增作业原因（说明与原作业的区别）'):''),source==='outbound'?'sop_need_from_outbound':'sop_need_create',v=>({...v,source_id,source_type:source,outbounds:readObs()}),r=>detail(r.id));const readObs=CKOptionalOutbounds($('optionalOutbounds'));
 $('editor').elements.department.value=doc.department;
}
function scanForm(record,pallet){
 current=record;
 form('扫码核对',input('pallet','托盘号／散箱位置','text',pallet)+input('barcode','条码（每箱扫描一次）'),'sop_check_scan',v=>v,async r=>{const latest=await api('sop_get',{id:r.id});await detail(r.id);scanForm(latest.record,pallet||'');});
 const f=$('editor'),original=f.onsubmit;f.onsubmit=e=>{pallet=f.elements.pallet.value;return original(e);};f.elements.barcode.focus();
}

let destroyed=false;
const start=async()=>{if(destroyed)return;try{if(options.id)await detail(options.id);else {await openSource();if(options.create)create(options.create);}}catch(e){notice(e.message);}if(!destroyed)poll=setInterval(checkUpdates,20000);};
start();
return {load,detail,create,taskForm,destroy(){destroyed=true;clearInterval(poll);closeModal();root.replaceChildren();}};
};
})();
