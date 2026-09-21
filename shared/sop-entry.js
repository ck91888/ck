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
  if(type==='inbound'&&r.items.length){
   head.className='ck-inline-heading ck-inbound-work card';head.innerHTML='<div class="card-title">本批作业要求 / 작업 지시</div><div class="ck-work-preview">'+CKWorkNeedsTable(r.items)+'</div><div class="ck-buttons"></div>';
   head.querySelector('.ck-buttons').append(button('查看整批作业指令',()=>{goView('need');mount(document.getElementById('view-need'),{tab:'need',group_source_id:id,context:'collab'});}));
   head.querySelector('.ck-buttons').append(button('追加本批作业要求',()=>{goView('need');mount(document.getElementById('view-need'),{tab:'need',source:type,source_id:id,supplement:true,context:'collab'});}));return;
  }
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
  const nav=document.createElement('nav');nav.className='ck-cross-nav';nav.innerHTML='<a href="/">CK</a><a href="/001/">现场执行</a><a href="/002/">协同中心</a><a href="/003/">耗材与物品</a><a href="/shuju/">数据看板</a><a href="/attendance/">上下班签到</a>';for(const a of nav.querySelectorAll('a'))if(a.getAttribute('href')===(app==='home'?'/':'/'+app+'/'))a.setAttribute('aria-current','page');document.body.insertBefore(nav,document.body.children[1]||null);
  if(app==='002'){
   const originalPager=window.renderPager;window.renderPager=function(key,res,reload){const html=originalPager(key,res,reload),p=getPager(key);return p.total<=p.limit?html.replace('class="pager-bar"','class="pager-bar pager-single"'):html;};
   document.querySelectorAll('.filter-bar').forEach(bar=>{const drawer=document.createElement('details');drawer.className='ck-filter-drawer';drawer.open=matchMedia('(min-width:701px)').matches;const summary=document.createElement('summary');summary.textContent='查询筛选 / 검색 필터';drawer.append(summary);bar.before(drawer);drawer.append(bar);});
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
   showMain();if(params.get('need')&&params.get('create_outbound'))goView('outbound_create');else if(params.get('need')){goTab('need');mount(v,{tab:'need',id:params.get('need'),context:'collab'});}else if(params.get('inbound'))openInboundDetail(params.get('inbound'));else if(params.get('issue'))openIssueDetail(params.get('issue'));else if(params.get('tab'))goTab(params.get('tab'));
  }else if(app==='001'){
   window.CKInstallDispatch();
   const iconPaths=['M2 6h12v11H2z M14 10h4l4 4v3h-8 M7 18a2 2 0 1 1-4 0 2 2 0 0 1 4 0 M21 18a2 2 0 1 1-4 0 2 2 0 0 1 4 0','M3 7l9-4 9 4v11l-9 4-9-4z M3 7l9 4 9-4 M12 11v11','M8 5H4v17h16V5h-4 M8 2h8v5H8z M8 12h8 M8 17h6','M3 4h11v15H3z M14 11h8 M18 7l4 4-4 4','M4 7h16 M4 12h16 M4 17h16 M8 4v6 M16 9v6 M10 14v6','M12 3L2 21h20z M12 9v5 M12 17v1','M3 12l7 2 2 7 2-7 7-11z M10 14l11-11','M4 3v18h18 M8 16v-5 M13 16V7 M18 16V4','M9 15l6-6 M8 17l-2 2a4 4 0 0 1-5-5l5-5 M16 7l2-2a4 4 0 0 1 5 5l-5 5'];
   document.querySelectorAll('.home-btn').forEach((b,i)=>{b.setAttribute('role','button');b.tabIndex=0;b.onkeydown=ev=>{if(ev.key==='Enter'||ev.key===' '){ev.preventDefault();b.click();}};const icon=b.querySelector('.icon');if(icon)icon.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="'+(iconPaths[i]||iconPaths[0])+'"/></svg>';});
   const renderPlan=window.renderUnloadPlanCard;window.renderUnloadPlanCard=function(data,no){renderPlan(data,no);const info=document.getElementById('unloadPlanInfo');const box=document.createElement('section');box.className='ck-inbound-work';box.innerHTML='<p>正在读取本批分货要求…</p>';info.append(box);request('sop_linked',{source_id:data.plan.id}).then(r=>{if(!box.isConnected)return;box.innerHTML=r.items.length?'<h3>卸货时的分货与操作要求</h3>'+CKWorkNeedsTable(r.items)+'<p class="ck-muted">打托货边卸边打托；散出货集中放置；入库代发货单独分出并交代发组。已完成内容在交接时说明，后续只做剩余操作。</p>':'<p class="ck-muted">本计划暂无单独作业要求。</p>';}).catch(e=>{box.textContent='作业要求读取失败：'+e.message;});};
   const tasks=document.createElement('section');tasks.className='ck-inline-heading';tasks.innerHTML='<b>现场在途任务（点击继续原单据操作）</b><div class="ck-buttons"></div>';document.getElementById('page-home').append(tasks);
   window.initHome=async()=>{CKClearNativeJob();document.getElementById('myTaskBar')?.classList.add('hidden');window._unloadPlanData=null;
    try{const r=await request('sop_dispatch_list');const list=tasks.querySelector('.ck-buttons');list.replaceChildren();for(const job of r.items)list.append(button((window.JOB_TYPE_LABEL?.[job.job_type]||job.job_type)+' · '+job.workers.map(w=>w.name).join('、')+' · '+job.id,()=>CKOpenNativeJob(job)));if(!r.items.length)list.textContent='暂无原业务在途派工；关联作业请进入按单操作 → 大货操作。';}catch(e){tasks.querySelector('.ck-buttons').textContent=e.message;}
   };

   document.getElementById('headerWorker').textContent=u.name+' · 负责人';document.getElementById('headerWorker').onclick=()=>{};
   const dispatch=document.createElement('div');dispatch.id='page-dispatch';dispatch.className='page';dispatch.innerHTML='<button class="nav-back" type="button">← 现场首页</button><div id="ck-dispatch-body"></div>';document.body.append(dispatch);dispatch.querySelector('button').onclick=()=>showPage('home');
   let fieldWork=null;
   const bulkPage=document.getElementById('page-bulk_op'),bulkHost=document.createElement('div');bulkPage.append(bulkHost);
   const originalBulkInit=window.initBulkOp;window.initBulkOp=function(){
    if(window._activeJobId&&!String(window._activeJobId).startsWith('SOPJOB-')){bulkHost.hidden=true;return originalBulkInit();}
    for(const node of ['bulkStateIdle','bulkStateWorking'])document.getElementById(node).style.display='none';
    bulkHost.hidden=false;if(fieldWork)fieldWork.destroy();fieldWork=CKFieldWork(bulkHost,params.get('task')||params.get('need')||'');
   };
   const labor=document.createElement('div');labor.id='page-labor';labor.className='page';labor.innerHTML='<div class="topbar"><button class="back-btn">← 现场首页</button><div class="page-title">人员与休息 / 인원·휴식</div></div><div id="ck-field-labor" style="padding:0 16px"></div>';document.body.append(labor);labor.querySelector('button').onclick=()=>showPage('home');
   window.CKOpenFieldLabor=async()=>{showPage('labor');await CKLabor(document.getElementById('ck-field-labor'),{field:true});};
   const laborButton=button('现场人员与休息 / 인원·휴식',window.CKOpenFieldLabor);laborButton.className='btn btn-outline';document.getElementById('page-home').prepend(laborButton);
   const switchFieldPage=window.showPage;window.showPage=function(name,...args){if(name!=='bulk_op'&&fieldWork){fieldWork.destroy();fieldWork=null;}return switchFieldPage(name,...args);};
   wrap('loadIssueDetail',issuePanel);
   const verifyStart=window.startVerifyScan;window.startVerifyScan=async function(btn){
    const id=document.getElementById('vsBatchSelect')?.value;if(!id)return verifyStart(btn);
    try{const r=await request('sop_check_for_batch',{legacy_id:id});if(!r.record)return verifyStart(btn);
     showPage('dispatch');mount(document.getElementById('ck-dispatch-body'),{tab:'check',id:r.record.id,context:'field',back:()=>goPage('verify_scan')});
    }catch(e){alert(e.message);}
   };
   showPage('home');
   if(params.get('task')||params.get('need')){showPage('bulk_op');}
  }else if(app==='shuju'){
   localStorage.removeItem(SHUJU_KEY_STORAGE);showApp();
   const b=button('派工质量与待办',()=>{document.querySelectorAll('.tab-content').forEach(x=>x.style.display='none');window._currentTab='sop';host.style.display='';mount(host,{tab:'dashboard',context:'dashboard'});});
   const bar=document.querySelector('.tab-bar');bar.append(b);const host=document.createElement('div');host.id='ck-dashboard';host.style.display='none';document.getElementById('appWrap').append(host);
   const people=document.createElement('div');people.id='ck-labor-dashboard';people.hidden=true;document.getElementById('appWrap').append(people);const peopleButton=button('日当人力 / 일용직',async()=>{document.querySelectorAll('.tab-content').forEach(x=>x.style.display='none');host.style.display='none';people.hidden=false;window._currentTab='labor';bar.querySelectorAll('button').forEach(x=>x.classList.remove('active'));peopleButton.classList.add('active');await CKLabor(people);});bar.prepend(peopleButton);
   const original=window.switchTab;window.switchTab=function(...args){host.style.display='none';people.hidden=true;return original(...args);};
   const realtime=window.loadRealtime;window.loadRealtime=async function(...args){const result=await realtime(...args);try{const r=await CKAttendance.api('summary');const a=document.getElementById('statActiveWorkers'),t=document.getElementById('statTodayLogins');a.textContent=r.items.filter(x=>!x.record.outAt).length;t.textContent=r.items.length;a.parentElement.querySelector('.stat-label').textContent='已签到未签退日当';a.parentElement.querySelector('.stat-sub').textContent='包含作业、休息与尚未分配人员';t.parentElement.querySelector('.stat-label').textContent='今日签到日当';t.parentElement.querySelector('.stat-sub').textContent='来自签到点的去重出勤人数';}catch(e){document.getElementById('statTodayLogins').textContent='读取失败';}return result;};await loadRealtime();
   b.addEventListener('click',()=>{people.hidden=true;bar.querySelectorAll('button').forEach(x=>x.classList.remove('active'));b.classList.add('active');});
   if(params.get('tab')==='labor')peopleButton.click();
  }else if(app==='003'){
   S003.key='';S003.role='admin';S003.operatorId=u.id;S003.operatorName=u.name;bootApp();
  }else if(app==='attendance'){
   await CKAttendanceKiosk();
  }else{
   const guide=document.createElement('div');guide.className='ck-demo-control';guide.innerHTML='<p>测试准备 / 테스트 준비 · 使用虚拟单据检验完整流程。</p><p id="ck-demo-result"></p>';guide.append(button('准备一组虚拟验收单据',async()=>{const r=await request('sop_demo_prepare',{client_req_id:'demo-v2'});document.getElementById('ck-demo-result').innerHTML='虚拟数据已准备。<a href="/002/?tab=inbound">从协同中心入库计划开始</a>。<br>工牌：TEST-A|测试操作员甲、TEST-B|测试操作员乙。';}));document.querySelector('.nav').append(guide);
  }
  if(app==='001'||app==='002'){
   const notices=document.createElement('div');notices.className='ck-updates';notices.hidden=true;nav.after(notices);
   const update=async()=>{if(document.hidden)return;try{const r=await request('sop_updates');notices.hidden=!r.items.length;notices.innerHTML=r.items.length?'<b>有 '+r.items.length+' 条最新要求待仓库确认</b> '+r.items.slice(0,5).map(x=>'<a href="/002/?issue='+encodeURIComponent(x.legacy_id)+'">'+esc(x.title.slice(0,36))+'</a>').join(' · '):'';}catch(e){notices.hidden=false;notices.textContent='消息同步失败，请刷新检查网络：'+e.message;}};await update();setInterval(update,15000);window.addEventListener('focus',update);
  }
 }
 setup().catch(e=>{document.documentElement.classList.remove('ck-auth-pending');const el=document.createElement('p');el.className='ck-updates';el.textContent='页面初始化失败：'+e.message;document.body.prepend(el);});
})();
