/* Upgrade features mounted in the original applications; no separate workbench. */
(function(){
 'use strict';if(!window.CK_SOP_ROLLOUT?.enabled||!window.CKSession)return;
 const app=location.pathname.split('/').filter(Boolean)[0]||'home';
 const params=new URLSearchParams(location.search);const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 let instance=null,activeRoot=null;
 const request=(action,data={})=>CKSession.request(action,data);
 function mount(root,options){if(instance)instance.destroy();activeRoot=root;instance=CKWorkflow(root,options);return instance;}
 function button(label,fn){const b=document.createElement('button');b.type='button';b.className='btn btn-outline';b.textContent=label;b.onclick=async()=>{if(b.disabled)return;b.disabled=true;try{await fn();}catch(e){alert(e.message);}finally{b.disabled=false;}};return b;}
 function panel(parent){let el=parent.querySelector('[data-ck-workflow]');if(!el){el=document.createElement('div');el.dataset.ckWorkflow='true';parent.prepend(el);}return el;}
 function block(parent,text){let el=parent.querySelector('.ck-inline-heading');if(el)return el;el=document.createElement('div');el.className='ck-inline-heading';el.innerHTML='<b>'+esc(text)+'</b><div class="ck-buttons"></div>';parent.prepend(el);return el;}
 function wrap(name,after){const original=window[name];if(typeof original!=='function')return;window[name]=async function(...args){const out=await original.apply(this,args);await after(...args);return out;};}
 async function sourcePanel(type,id,target){
  const body=document.getElementById(target);if(!body||!id)return;
  const r=await request('sop_linked',{source_id:id});
  const head=block(body,'关联作业：操作要求、结果和数量在这里统一追踪');const buttons=head.querySelector('.ck-buttons');buttons.replaceChildren();
  for(const n of r.items){const b=button(n.title+' · '+n.status,()=>{goView('need');mount(document.getElementById('view-need'),{tab:'need',id:n.id,context:'collab'});});buttons.append(b);}
  buttons.append(button(r.items.length?'新增补充作业（需说明原因）':'建立到货后作业需求',()=>{goView('need');mount(document.getElementById('view-need'),{tab:'need',source:type,source_id:id,supplement:r.items.length>0,context:'collab'});}));
 }
 async function issuePanel(){
  const id=window._currentIssueId,body=document.getElementById('issueDetailBody');if(!id||!body)return;
  const r=await request('sop_issue_adopt',{legacy_id:id,native:true,client_req_id:crypto.randomUUID()});
  // The original attachment/history area remains, but all current edits share one revision.
  body.querySelectorAll('button[onclick]').forEach(b=>{if(/completeIssue|reworkIssue|cancelIssue|handleIssueFinish/.test(b.getAttribute('onclick')))b.hidden=true;});
  mount(panel(body),{tab:'issue',id:r.id,context:app==='001'?'field':'collab',back:()=>app==='001'?goPage('issue_list'):goTab('issue')});
 }
 async function checkPanel(){
  const body=document.getElementById('checkDetailBody'),id=window._currentVerifyBatchId;if(!body||!id)return;
  const r=await request('sop_check_for_batch',{legacy_id:id});
  if(r.record){body.replaceChildren();mount(panel(body),{tab:'check',id:r.record.id,back:()=>goTab('check')});}
  else {const head=block(body,'按出库日期追加、撤销和分轮核对');head.querySelector('.ck-buttons').append(button('设置出库日期并继续此批次',()=>mount(panel(body),{tab:'check',source:'check',source_id:id})));}
 }
 async function outboundPicker(){
  if(document.getElementById('ck-completed-need'))return;
  const field=document.getElementById('oc-instruction');if(!field)return;
  const box=document.createElement('div');box.className='ck-inline-heading';box.innerHTML='<label>操作需求来源 / 작업 요청 출처<select id="ck-completed-need"><option value="">本次新增操作，或无需操作</option></select></label><label id="ck-link-quantity-label" hidden>使用已完成成果数量<input id="ck-link-quantity" type="number" min="1"></label><p id="ck-link-note">新操作按出库计划要求建立一份关联作业；已有成果请在这里选择，避免重复操作。</p>';
  field.closest('.form-group').before(box);
  const list=await request('sop_list',{kind:'need'});const select=document.getElementById('ck-completed-need');
  for(const x of list.items.filter(x=>x.result&&['waiting_customer','linked'].includes(x.status))){const remaining=x.result.quantity-(x.links||[]).reduce((n,v)=>n+v.quantity,0);if(remaining<=0)continue;const option=document.createElement('option');option.value=x.id;option.textContent=x.customer+' · '+x.title+' · 剩余 '+remaining+x.result.unit;option._need=x;select.append(option);}
  select.onchange=()=>{const n=select.selectedOptions[0]?._need;document.getElementById('ck-link-quantity-label').hidden=!n;field.readOnly=!!n;const uses=document.getElementById('oc-uses-stock-op');if(uses)uses.disabled=!!n;if(n){document.getElementById('oc-customer').value=n.customer;document.getElementById('oc-biz-class').value=n.department;field.value=n.instructions;if(uses)uses.value='0';document.getElementById('ck-link-quantity').value=n.result.quantity-(n.links||[]).reduce((s,x)=>s+x.quantity,0);document.getElementById('ck-link-note').textContent='引用原作业成果，不再重复派工。单位：'+n.result.unit;}else document.getElementById('ck-link-note').textContent='本次新增操作将建立一份关联作业。';};
  if(params.get('need')&&Array.from(select.options).some(x=>x.value===params.get('need'))){select.value=params.get('need');select.onchange();}
 }
 // Disable only automatic credential restoration in staging. The original UI and
 // business functions still run; a shared HttpOnly session authorizes their requests.
 if(app==='002')window.checkAutoLogin=()=>{};
 if(app==='shuju')window.checkAuth=()=>{};
 if(app==='001')window.checkBadgeAuth=()=>false;
 if(app==='001'||app==='002'||app==='shuju')window.doLogout=()=>CKSession.logout();
 const nativeApi=window.api;
 if(nativeApi&&app==='002')window.api=async function(body){
  if(body.action==='v2_outbound_order_create'){
   const id=document.getElementById('ck-completed-need')?.value;
   if(id&&document.getElementById('view-outbound_create')?.style.display!=='none'){body.sop_existing_need_id=id;body.sop_link_quantity=Number(document.getElementById('ck-link-quantity').value);body.uses_stock_operation=0;}
  }
  return nativeApi(body);
 };
 async function setup(){
  const u=await CKSession.ready;
  const nav=document.createElement('nav');nav.className='ck-cross-nav';nav.innerHTML='<a href="/001/">现场执行</a><a href="/002/">协同中心</a><a href="/003/">耗材管理</a><a href="/shuju/">数据看板</a>';document.body.insertBefore(nav,document.body.children[1]||null);
  if(app==='002'){
   localStorage.removeItem(V2_KEY_STORAGE);setUser(u.name);window.promptUserName=()=>{};
   const tabs=document.getElementById('mainTabs'),inbound=tabs.querySelector('[data-tab=inbound]'),outbound=tabs.querySelector('[data-tab=outbound]');tabs.insertBefore(inbound,outbound);
   LANG.zh.tab_outbound='出库计划';LANG.ko.tab_outbound='출고 계획';LANG.zh.app_subtitle='入库计划 · 作业需求 · 出库计划 · 问题沟通 · 核对';
   const needButton=button('作业需求 / 작업 요청',()=>goTab('need'));needButton.dataset.tab='need';tabs.insertBefore(needButton,outbound);
   const v=document.createElement('div');v.id='view-need';v.className='view';v.style.display='none';document.getElementById('page-main').append(v);
   const originalGoTab=window.goTab;window.goTab=function(tab,b){originalGoTab(tab,b);if(tab==='need')mount(v,{tab:'need',context:'collab'});};
   wrap('loadInboundDetail',()=>sourcePanel('inbound',_currentInboundId,'inboundDetailBody'));
   wrap('loadOutboundDetail',()=>sourcePanel('outbound',_currentOutboundId,'outboundDetailBody'));
   wrap('loadIssueDetail',issuePanel);wrap('loadVerifyDetail',checkPanel);
   wrap('loadVerifyList',async()=>{const view=document.getElementById('view-check');const head=block(view,'按出库日期管理总清单 · 11:00 / 13:00 / 16:00 分轮核对');if(!head.querySelector('button'))head.querySelector('.ck-buttons').append(button('打开日期清单／新建清单',()=>{const body=document.getElementById('checkListBody');mount(body,{tab:'check',context:'collab'});}));});
   document.getElementById('btnNewCheck').onclick=()=>{goView('check');mount(document.getElementById('checkListBody'),{tab:'check',context:'collab',create:'check'});};
   const originalGoView=window.goView;window.goView=function(name){originalGoView(name);if(name==='outbound_create')outboundPicker().catch(e=>alert(e.message));};
   const wh=document.createElement('section');wh.className='ck-inline-heading ck-workflow ck-work-plans';document.getElementById('ibc-remark').closest('.form-group').after(wh);window.CKInboundWorks=CKWorkFields(wh);
   showMain();if(params.get('need')&&params.get('create_outbound'))goView('outbound_create');else if(params.get('need')){goTab('need');mount(v,{tab:'need',id:params.get('need'),context:'collab'});}else if(params.get('issue'))openIssueDetail(params.get('issue'));else if(params.get('tab'))goTab(params.get('tab'));
  }else if(app==='001'){
   window.CKInstallDispatch();
   const tasks=document.createElement('section');tasks.className='ck-inline-heading';tasks.innerHTML='<b>现场在途任务（点击继续原单据操作）</b><div class="ck-buttons"></div>';document.getElementById('page-home').append(tasks);
   window.initHome=async()=>{CKClearNativeJob();document.getElementById('myTaskBar')?.classList.add('hidden');window._unloadPlanData=null;
    try{const r=await request('sop_dispatch_list');const list=tasks.querySelector('.ck-buttons');list.replaceChildren();for(const job of r.items)list.append(button((window.JOB_TYPE_LABEL?.[job.job_type]||job.job_type)+' · '+job.workers.map(w=>w.name).join('、')+' · '+job.id,()=>CKOpenNativeJob(job)));if(!r.items.length)list.textContent='暂无原业务在途派工；关联作业请进入负责人派工与审核。';}catch(e){tasks.querySelector('.ck-buttons').textContent=e.message;}
   };

   document.getElementById('headerWorker').textContent=u.name+' · 负责人';document.getElementById('headerWorker').onclick=()=>{};
   const dispatch=document.createElement('div');dispatch.id='page-dispatch';dispatch.className='page';dispatch.innerHTML='<button class="nav-back" type="button">← 现场首页</button><div id="ck-dispatch-body"></div>';document.body.append(dispatch);dispatch.querySelector('button').onclick=()=>showPage('home');
   const home=document.querySelector('.home-grid');const b=document.createElement('div');b.className='home-btn accent';b.innerHTML='<div class="icon">👥</div><div class="label">负责人派工与审核</div><div class="label-ko">담당자 배정·검수</div>';b.onclick=()=>{showPage('dispatch');mount(document.getElementById('ck-dispatch-body'),{tab:'task',context:'field'});};home.prepend(b);
   wrap('loadIssueDetail',issuePanel);
   const verifyStart=window.startVerifyScan;window.startVerifyScan=async function(btn){
    const id=document.getElementById('vsBatchSelect')?.value;if(!id)return verifyStart(btn);
    try{const r=await request('sop_check_for_batch',{legacy_id:id});if(!r.record)return verifyStart(btn);
     showPage('dispatch');mount(document.getElementById('ck-dispatch-body'),{tab:'check',id:r.record.id,context:'field',back:()=>goPage('verify_scan')});
    }catch(e){alert(e.message);}
   };
   showPage('home');
   if(params.get('task')||params.get('need')){showPage('dispatch');mount(document.getElementById('ck-dispatch-body'),{tab:params.get('task')?'task':'need',id:params.get('task')||params.get('need'),context:'field'});}
  }else if(app==='shuju'){
   localStorage.removeItem(SHUJU_KEY_STORAGE);showApp();
   const b=button('派工质量与待办',()=>{document.querySelectorAll('.tab-content').forEach(x=>x.style.display='none');window._currentTab='sop';host.style.display='';mount(host,{tab:'dashboard',context:'dashboard'});});
   const bar=document.querySelector('.tab-bar');bar.append(b);const host=document.createElement('div');host.id='ck-dashboard';host.style.display='none';document.getElementById('appWrap').append(host);
   const original=window.switchTab;window.switchTab=function(...args){host.style.display='none';return original(...args);};
  }else if(app==='003'){
   S003.key='';S003.role='admin';S003.operatorId=u.id;S003.operatorName=u.name;bootApp();
  }else{
   const guide=document.createElement('div');guide.className='ck-demo-control';guide.innerHTML='<p>四个入口均为原系统页面，数据独立于正式仓库。</p><p id="ck-demo-result"></p>';guide.append(button('准备一组虚拟验收单据',async()=>{const r=await request('sop_demo_prepare',{client_req_id:'demo-v2'});document.getElementById('ck-demo-result').innerHTML='虚拟数据已准备。<a href="/002/?tab=inbound">从协同中心入库计划开始</a>。<br>工牌：TEST-A|测试操作员甲、TEST-B|测试操作员乙。';}));document.querySelector('.nav').append(guide);
  }
  if(app==='001'||app==='002'){
   const notices=document.createElement('div');notices.className='ck-updates';notices.hidden=true;nav.after(notices);
   const update=async()=>{if(document.hidden)return;try{const r=await request('sop_updates');notices.hidden=!r.items.length;notices.innerHTML=r.items.length?'<b>有 '+r.items.length+' 条最新要求待仓库确认</b> '+r.items.slice(0,5).map(x=>'<a href="/002/?issue='+encodeURIComponent(x.legacy_id)+'">'+esc(x.title.slice(0,36))+'</a>').join(' · '):'';}catch(e){notices.hidden=false;notices.textContent='消息同步失败，请刷新检查网络：'+e.message;}};await update();setInterval(update,15000);window.addEventListener('focus',update);
  }
 }
 setup().catch(e=>{document.documentElement.classList.remove('ck-auth-pending');const el=document.createElement('p');el.className='ck-updates';el.textContent='页面初始化失败：'+e.message;document.body.prepend(el);});
})();
