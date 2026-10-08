/* Shared real attendance UI. All writes are persisted by the staging Worker. */
(function(){'use strict';
const e=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let agencies=[];
const at=t=>t?new Date(t).toLocaleTimeString('en-GB',{timeZone:'Asia/Seoul',hour:'2-digit',minute:'2-digit'}):'—',hours=n=>{const m=Math.round(n||0);return Math.floor(m/60)+':'+String(m%60).padStart(2,'0');},company=a=>a==='동인천'?'동인천（东仁川）':a==='直招'?'直招（직접채용）':a,dept=d=>({bulk:'大货',direct_ship:'代发',import:'进口'}[d]||'其他');
const api=(action,data={})=>CKSession.request('sop_attendance_'+action,data);
const loadConfig=async()=>{const config=await api('config');agencies=config.agencies;return config;};
const jobSummary=x=>window.CKDocumentLabels.jobSummary(x);
const jobContext=(x,includeType=false)=>[x.has_business_reference===false?'':jobDepartment(x.department),includeType&&x.has_business_reference!==false?window.CKDocumentLabels.jobType(x):'',jobSummary(x)].filter(Boolean).join(' · ');
function radios(current=''){return '<div class="agencies">'+agencies.map(a=>`<label class="agency"><input type="radio" name="agency" value="${e(a)}" ${a===current?'checked':''} required><span>${e(a)}</span><small>${a==='동인천'?'东仁川':a==='直招'?'직접채용':'인력회사 / 人力公司'}</small></label>`).join('')+'</div>';}
function badge(r){qrcode.stringToBytes=qrcode.stringToBytesFuncs['UTF-8'];const qr=qrcode(0,'M');qr.addData(r.badgeId+'|'+r.name);qr.make();return `<div class="ck-label"><div class="badge-qr">${qr.createSvgTag({cellSize:4,margin:16,scalable:true})}</div><div class="badge-info"><div class="badge-heading"><b>CK</b><span>명찰 / 工牌</span></div><div class="badge-name ${r.name.length>22?'very-long':r.name.length>9?'long':''}">${e(r.name)}</div>${r.badgeType==='permanent'?'':`<div class="badge-agency">${e(company(r.agency))}</div>`}<div class="badge-date">${r.personType==='employee'?'职员 / 직원':r.badgeType==='permanent'?'长期 / 고정':e(r.day)}</div><div class="badge-date">${e(r.employeeNo||r.badgeId.split('-').pop())}</div></div></div>`;}
async function print(r){if(r.inAt&&r.id){const x=await api('print',{id:r.id,client_req_id:crypto.randomUUID()});r=x.record;}let root=document.getElementById('print-root');if(!root){root=document.createElement('div');root.id='print-root';document.body.append(root);}root.innerHTML=badge(r);const style=document.createElement('style');style.textContent='@page{size:70mm 30mm;margin:0}';document.head.append(style);document.body.classList.add('ck-print-badge');try{window.print();}finally{document.body.classList.remove('ck-print-badge');style.remove();}return r;}
// Keep the same request id for a retry after a network error. A business rejection can be corrected.
function bind(form,action,values,done){let requestId='',signature='';form.onsubmit=async ev=>{ev.preventDefault();const buttons=[...form.querySelectorAll('button')],err=form.querySelector('[role=alert]');try{const data=values(),sig=JSON.stringify(data);if(sig!==signature||!requestId){signature=sig;requestId=crypto.randomUUID();}buttons.forEach(b=>b.disabled=true);if(err){err.hidden=true;err.textContent='';}const r=await api(action,{...data,client_req_id:requestId});await done(r);}catch(x){if(x.businessError)requestId='';if(err){err.hidden=false;err.textContent=x.message;}}finally{buttons.forEach(b=>b.disabled=false);}};}
const managementLabels={'':'未标记 / 미지정',bulk:'大货 / 대량',direct_ship:'代发 / 직배송',import:'进口 / 수입'};
const managementLabel=value=>managementLabels[value]||managementLabels[''];
const jobDepartment=value=>({bulk:'大货 / 대량',direct_ship:'代发 / 직배송',import:'进口 / 수입'}[value]||'其他 / 기타');
const statusLabels={unassigned:'未分配作业 / 배정 대기',working:'作业中 / 작업 중',rest:'休息中 / 휴식 중',out:'已下班 / 퇴근'};
function statusMarkup(item){const state=Object.hasOwn(statusLabels,item?.status)?item.status:'unknown',label=statusLabels[state]||'状态待核实 / 상태 확인 필요';return `<div class="ck-current-status"><span class="ck-work-status" data-status="${state}">${e(label)}</span>${state==='working'?(item.currentJobs||[]).map(j=>`<small class="ck-current-job">${e(jobContext(j))}</small>`).join(''):''}</div>`;}
// A dashboard's clock is not evidence that its report has been read again.
// Only an applied, successful summary response advances this timestamp.
function reportRefresh({root,config,alive,visible,date,paused,refresh,active=()=>true}){
 const interval=15000,staleAfter=30000,serverTime=Date.parse(config.asOf),offset=Number.isFinite(serverTime)?serverTime-Date.now():0;
 const node=root.querySelector('[data-report-freshness]');
 let timer=null,disposed=false,suspended=false,listening=false,inFlight=0,nextRead=Date.now()+interval,due=false,lastAsOf='',failed=false;
 const today=()=>new Date(Date.now()+offset+32400000).toISOString().slice(0,10);
 const current=()=>active()&&date()===today();
 const usable=()=>!disposed&&!suspended&&alive()&&visible()&&!document.hidden;
 const canApply=()=>usable()&&current()&&!paused();
 const canRead=()=>canApply()&&!inFlight;
 function paint(){
  if(disposed||!alive()||!node)return;
  const stamp=Date.parse(lastAsOf),known=Number.isFinite(stamp),old=failed||(known&&current()&&Date.now()+offset-stamp>=staleAfter);
  const time=known?new Date(stamp+32400000).toISOString().replace('T',' ').replace('Z',' KST'):'';
  let text=time?`作业状态更新于 ${time} / 작업 상태 갱신`:'尚未成功读取作业状态 / 작업 상태를 아직 불러오지 못했습니다';
  if(failed)text+=' · 刷新失败，'+(known?'保留上次数据，状态可能已过时':'请重试')+' / 갱신 실패 · '+(known?'이전 데이터, 상태가 오래되었을 수 있습니다':'다시 시도하세요');
  else if(old)text+=' · 状态可能已过时 / 오래된 상태일 수 있습니다';
  if(current()&&paused())text+=' · 查看或编辑中，自动更新已暂停 / 확인·수정 중 자동 갱신 일시 중지';
  else if(!current())text+=' · 历史日期不自动更新 / 과거 날짜는 자동 갱신하지 않습니다';
  if(node.textContent!==text)node.textContent=text;
  const state=failed?'failed':old?'stale':known?'current':'loading';
  if(node.dataset.freshness!==state)node.dataset.freshness=state;
  const cls=old?'ck-warning':'ck-muted';if(node.className!==cls)node.className=cls;
  if(node.hidden===active())node.hidden=!active();
 }
 function check(force=false){
  if(disposed)return;
  if(!alive()){destroy();return;}
  if(force)due=true;
  paint();
  if((due||Date.now()>=nextRead)&&canRead())void refresh({background:true});
 }
 const returned=()=>{if(!document.hidden)check(true);};
 function start(){
  if(disposed||listening)return;suspended=false;listening=true;
  window.addEventListener('focus',returned);document.addEventListener('visibilitychange',returned);
  timer=setInterval(()=>check(true),interval);
 }
 function suspend(){
  suspended=true;due=true;clearInterval(timer);timer=null;listening=false;
  window.removeEventListener('focus',returned);document.removeEventListener('visibilitychange',returned);
 }
 const restored=()=>{if(suspended){start();check(true);}};
 function destroy(){if(disposed)return;disposed=true;suspend();observer.disconnect();window.removeEventListener('pagehide',suspend);window.removeEventListener('pageshow',restored);}
 // Open/close and import-panel visibility changes are observed without replacing
 // the dialog or its contents. A due read resumes once the last review is closed.
 const observer=new MutationObserver(()=>check());
 root.querySelectorAll('dialog,[data-import-panel]').forEach(el=>observer.observe(el,{attributes:true,attributeFilter:['open','hidden']}));
 window.addEventListener('pagehide',suspend);window.addEventListener('pageshow',restored);start();paint();
 return {today,canRead,canApply,check,destroy,
  begin(){inFlight++;due=false;nextRead=Date.now()+interval;paint();},
  success(asOf){lastAsOf=asOf||'';failed=false;due=false;nextRead=Date.now()+interval;paint();},
  failure(){failed=true;due=false;paint();},
  defer(){due=true;paint();},
  end(){inFlight=Math.max(0,inFlight-1);paint();if(due&&canRead())check();},
  reset(){lastAsOf='';failed=false;due=false;paint();}
 };
}
function preserveReportView(root,render){
 const focused=document.activeElement,inside=focused&&root.contains(focused),attrs=inside?[...focused.attributes].filter(a=>a.name.startsWith('data-')).map(a=>[a.name,a.value]):[];
 const card=inside&&!!focused.closest('.ck-person-cards'),x=window.scrollX,y=window.scrollY,rootX=root.scrollLeft,rootY=root.scrollTop;
 const tables=[...root.querySelectorAll('.ck-table-wrap')].map(el=>[el.scrollLeft,el.scrollTop]);
 render();
 if(inside&&!focused.isConnected){
  const replacement=attrs.length?[...root.querySelectorAll(focused.tagName)].find(el=>attrs.every(([k,v])=>el.getAttribute(k)===v)&&!!el.closest('.ck-person-cards')===card):null;
  (replacement&&!replacement.disabled?replacement:root.querySelector('[data-refresh]'))?.focus({preventScroll:true});
 }
 root.scrollLeft=rootX;root.scrollTop=rootY;
 [...root.querySelectorAll('.ck-table-wrap')].forEach((el,i)=>{if(tables[i]){el.scrollLeft=tables[i][0];el.scrollTop=tables[i][1];}});
 if(window.scrollX!==x||window.scrollY!==y)window.scrollTo(x,y);
}
window.CKAttendance={api,badge,print,at,hours,statusMarkup,statusLabels,reportRefresh,preserveReportView};
window.CKAttendanceKiosk=async function(){
 await CKSession.ready;const config=await loadConfig(),root=document.getElementById('terminal-content');let mode='in',selected=null,timer=null;const el=id=>document.getElementById(id);
 const error='<p class="error" role="alert" hidden></p>',back='<button type="button" class="text-button" data-back>返回 / 돌아가기</button>';
 const clockOffset=Date.parse(config.asOf)-Date.now();function clock(){const d=new Date(Date.now()+clockOffset);el('kiosk-time').textContent=at(d);el('kiosk-date').textContent=d.toLocaleDateString('zh-CN',{timeZone:'Asia/Seoul',year:'numeric',month:'2-digit',day:'2-digit',weekday:'short'})+' · 한국 시간';}clock();setInterval(clock,1000);
 function page(html){clearTimeout(timer);root.innerHTML=html;root.querySelector('[data-back]')?.addEventListener('click',()=>switchMode(mode));}
 function switchMode(m){mode=m;selected=null;el('tab-in').setAttribute('aria-selected',m==='in');el('tab-out').setAttribute('aria-selected',m==='out');m==='in'?checkin():lookup('checkout');}
 function checkin(){page(`<form class="form-body"><button type="button" class="permanent-entry" id="fixed">已有长期工牌？扫码签到 <span>고정 명찰로 출근 등록</span></button><label for="name" class="field-label">姓名 <span>이름</span></label><input id="name" class="text-input" autocomplete="off" maxlength="40" placeholder="请输入姓名 / 이름을 입력하세요" required><fieldset><legend class="field-label">人力公司 <span>인력회사</span></legend>${radios()}</fieldset>${error}<button class="primary">签到 <span>출근 등록</span></button><div class="helper-row"><button type="button" class="text-button" id="company">修改人力公司 / 회사 변경</button><button type="button" class="text-button" id="reprint">补打工牌 / 명찰 재출력</button></div></form>`);
  bind(root.querySelector('form'),'checkin',()=>({name:el('name').value,agency:root.querySelector('input[name=agency]:checked')?.value}),r=>{if(r.identityRequired){const name=el('name').value;page(`<div class="form-body"><h1 class="find-title">今天已有同名记录</h1><p>오늘 같은 이름의 출근 기록이 있습니다.</p><p class="soft-note">已签到但工牌丢失，可以按姓名查找并补打。<br>명찰을 분실했으면 이름으로 검색해 재출력하세요.<br>如果是同名的另一位人员，请工作人员核实。<br>동명이인은 담당자에게 확인하세요.</p><button class="primary" id="verify">按姓名查找工牌 / 이름으로 명찰 찾기</button>${back}</div>`);el('verify').onclick=()=>reprintSearch(name);return;}selected=r.record;success('in',r.existing);});
  el('fixed').onclick=()=>lookup('fixed');el('company').onclick=()=>lookup('company');el('reprint').onclick=()=>reprintSearch(el('name').value);el('name').focus();
 }
 function reprintSearch(name='',agency=''){
  page(`<div class="form-body reprint-body"><h1 class="find-title">补打工牌 <span>명찰 재출력</span></h1><p class="soft-note">工牌丢了也可以：输入签到时的姓名，找到自己的记录。<br>명찰이 없어도 출근 등록한 이름으로 찾을 수 있어요.</p><form><label class="field-label" for="find-name">姓名 <span>이름</span></label><input id="find-name" class="text-input" value="${e(name)}" maxlength="40" autocomplete="off" required placeholder="完整姓名 / 성명"><label class="field-label" for="find-agency">人力公司 <span>인력회사 · 可选 / 선택사항</span></label><select id="find-agency" class="text-input"><option value="">全部公司 / 전체</option>${agencies.map(a=>`<option value="${a}" ${a===agency?'selected':''}>${e(company(a))}</option>`).join('')}</select>${error}<button class="primary">查找工牌 / 명찰 찾기</button></form><div id="find-results" aria-live="polite"></div><details class="reprint-scan"><summary>也可扫描或输入工号 / 명찰 번호로 찾기</summary><form id="scan-reprint"><label for="find-code">工牌编号 / 명찰 번호</label><input id="find-code" class="text-input" autocomplete="off" required placeholder="DA-… / DAF-…">${error}<button class="secondary">查找 / 찾기</button></form></details>${back}</div>`);
  const terms=()=>({name:el('find-name').value,agency:el('find-agency').value});
  bind(root.querySelector('form'),'search',terms,r=>{const query=terms(),box=el('find-results');box.innerHTML=r.items.length?`<p class="find-count">找到 ${r.items.length} 条，请核对后选择自己。<br>${r.items.length}건 · 본인 기록을 선택하세요.</p><div class="badge-candidates">${r.items.map((x,i)=>{const v=x.record||x.person;return `<button type="button" class="badge-candidate" data-candidate="${i}"><span><strong>${e(v.name)}</strong><span>${e(company(v.agency))}</span><small>${x.record?`今日 ${at(v.inAt)} 签到 · ${v.outAt?'已下班 / 퇴근':'已签到 / 출근'}`:'长期工牌 · 今日未签到 / 고정 명찰 · 오늘 미출근'}</small><small>工牌尾号 / 명찰 끝자리 ${e(v.badgeId.split('-').pop())}</small></span><b>核对 →<small>확인</small></b></button>`;}).join('')}</div>`:'<p class="find-empty">没有找到记录。请核对姓名和公司；临时工牌仅可补打当天记录。<br>기록이 없습니다. 이름과 회사를 확인하세요. 일일 명찰은 당일 기록만 조회됩니다.</p>';box.querySelectorAll('[data-candidate]').forEach(b=>b.onclick=async()=>{b.disabled=true;try{const x=await api('lookup',{badge:r.items[Number(b.dataset.candidate)].person.badgeId});reprintReview(x,query);}catch(x){box.insertAdjacentHTML('beforeend',`<p class="error" role="alert">${e(x.message)}</p>`);b.disabled=false;}});});
  bind(el('scan-reprint'),'lookup',()=>({badge:el('find-code').value}),r=>reprintReview(r,terms()));el('find-name').focus();
 }
 function reprintReview(x,query){
  if(!x.record&&x.person.badgeType!=='permanent')throw Error('没有当天签到记录 / 오늘 출근 기록이 없습니다');
  selected=x.record||x.person;const r=selected;
  page(`<div class="form-body reprint-review"><h1 class="find-title">核对并补打 <span>확인 후 재출력</span></h1><h2>${e(r.name)}</h2><p class="record-meta">${e(company(r.agency))}<br>${r.inAt?`${e(r.day)} · 上班 / 출근 ${at(r.inAt)}${r.outAt?` · 下班 / 퇴근 ${at(r.outAt)}`:''}`:'长期工牌 · 今日未签到 / 고정 명찰 · 오늘 미출근'}</p><div class="badge-preview">${badge(r)}</div><p class="soft-note">补打原工牌，保留原上下班时间。<br>기존 명찰만 재출력하며 출퇴근 기록은 바뀌지 않습니다.</p>${error}<button type="button" class="primary" id="confirm-reprint">这是我，打印工牌 / 본인 확인 · 출력</button><p class="soft-note" id="reprint-note" role="status"></p><div class="helper-row"><button type="button" class="text-button" id="find-again">重新查找 / 다시 찾기</button><button type="button" class="text-button" id="reprint-done">返回签到 / 출근 화면</button></div></div>`);
  el('find-again').onclick=()=>reprintSearch(query.name,query.agency);el('reprint-done').onclick=()=>switchMode('in');
  el('confirm-reprint').onclick=async()=>{const b=el('confirm-reprint'),err=root.querySelector('[role=alert]');b.disabled=true;err.hidden=true;try{selected=await print(selected);el('reprint-note').textContent='已打开打印，请确认实际出纸。没有新增签到。 / 출력 창이 열렸습니다. 실제 출력을 확인하세요. 출근 기록은 추가되지 않았습니다.';}catch(x){err.textContent=x.message;err.hidden=false;}finally{b.disabled=false;}};
 }
 function lookup(purpose){const out=purpose==='checkout',title=out?'请扫描工牌下班 / 퇴근 명찰 스캔':purpose==='fixed'?'长期日当 · 扫码签到 / 고정 명찰 출근':'扫描工牌 / 명찰 스캔';page(`<form class="form-body"><div class="checkout-intro"><h1>${title}</h1><p>将工牌对准扫码器 / 명찰을 스캐너에 대세요</p></div><label class="field-label" for="code">工牌 <span>명찰</span></label><input id="code" class="text-input" required autocomplete="off" placeholder="扫描后回车 / 스캔 후 Enter">${error}<button class="primary">${out?'确认下班 / 퇴근 등록':'确认 / 확인'}</button>${back}</form>`);
  bind(root.querySelector('form'),out?'checkout':'lookup',()=>({badge:el('code').value}),r=>{if(out){selected=r.record;success('out',r.existing,r.needsReview);return;}if(purpose==='fixed'){if(r.person.badgeType!=='permanent')throw Error('请使用长期工牌 / 고정 명찰을 사용하세요');if(r.record){selected=r.record;success(r.record.outAt?'out':'in',true);}else fixed(r.person);return;}if(!r.record)throw Error('没有当天签到记录 / 오늘 출근 기록이 없습니다');selected=r.record;purpose==='company'?editCompany():success(selected.outAt?'out':'in',true);});el('code').focus();
 }
 function fixed(p){page(`<form class="form-body"><h1>${e(p.name)}</h1><p>确认今天的人力公司 / 오늘의 인력회사를 확인하세요</p>${radios(p.agency)}${error}<button class="primary">签到 / 출근 등록</button>${back}</form>`);bind(root.querySelector('form'),'checkin',()=>({badge:p.badgeId,agency:root.querySelector('input:checked').value}),r=>{selected=r.record;success(selected.outAt?'out':'in',r.existing);});}
 function editCompany(){page(`<form class="form-body"><h1 class="find-title">${e(selected.name)} · 修改公司</h1><p>인력회사를 변경하세요.</p>${radios(selected.agency)}<p class="soft-note">更正当天公司，保留工牌和签到时间。<br>당일 회사만 변경됩니다. 출근 시간은 유지됩니다.</p>${error}<button class="primary">保存 / 저장</button>${back}</form>`);bind(root.querySelector('form'),'company',()=>({id:selected.id,version:selected.version,agency:root.querySelector('input:checked').value}),r=>{selected=r.record;success(selected.outAt?'out':'in',true);});}
 function success(kind,existing,review){const out=kind==='out',r=selected;page(`<div class="success-panel" role="status"><div class="success-symbol">✓</div><h1>${e(r.name)}，${out?'已下班':'已签到'}</h1><p class="korean">${e(r.name)}님, ${out?'퇴근':'출근'} 등록이 완료되었습니다.</p><p class="greeting">${out?'今天辛苦了。':'美好的一天要加油哦。'}<br>${out?'오늘도 수고하셨습니다.':'오늘도 좋은 하루 보내세요. 힘내세요!'}</p><div class="record-meta">${e(company(r.agency))} · ${at(out?r.outAt:r.inAt)}</div>${out?'':`<div class="badge-preview">${badge(r)}</div><p class="print-note">${r.badgeType==='permanent'?'固定工牌可继续使用 / 고정 명찰을 계속 사용하세요':'70 × 30 mm · 请保管至下班 / 퇴근 시까지 보관하세요'}</p>`}<div class="success-actions">${out?'':`<button id="print" class="${r.badgeType==='permanent'?'secondary':'primary'}">${r.badgeType==='permanent'?'补打':'打印'}工牌 / 명찰 출력</button><button id="edit" class="secondary">修改公司 / 회사 변경</button>`}<button id="next" class="secondary">下一位 / 다음 분</button></div><p id="print-note" class="soft-note">${review?'个人作业计时已停止，负责人需核实；同组任务继续。':existing?'使用原记录，未重复签到或签退。 / 중복 기록 없이 처리되었습니다.':''}</p><p role="alert" class="error" hidden></p></div>`);
  el('next').onclick=()=>switchMode(out?'out':'in');if(out)timer=setTimeout(()=>switchMode('out'),8000);else{el('edit').onclick=editCompany;el('print').onclick=async()=>{const b=el('print');b.disabled=true;try{selected=await print(selected);el('print-note').textContent='已打开打印，请确认实际出纸。取消或卡纸可补打，不会重复签到。 / 실제 출력 여부를 확인하세요.';}catch(x){const p=root.querySelector('[role=alert]');p.textContent=x.message;p.hidden=false;}finally{b.disabled=false;}};}
 }
 el('tab-in').onclick=()=>switchMode('in');el('tab-out').onclick=()=>switchMode('out');switchMode('in');
};
window.CKLabor=async function(root,{field=false}={}){
 // A tab can be opened again while its previous config/refresh is still in flight.
 root.__ckLabor?.destroy();
 let disposed=false,report=null,selectedId='',readVersion=0,editorVersion=0,mutating=false;
 let detailOrigin=null,editorOrigin=null,detailScroll=null,freshness;
 const retries=new Map(),lifecycle={destroy(){disposed=true;readVersion++;editorVersion++;observer?.disconnect();freshness?.destroy();window.removeEventListener('pagehide',leave);root.querySelectorAll('dialog[open]').forEach(d=>d.close());}};
 let observer;root.__ckLabor=lifecycle;
 const alive=()=>!disposed&&root.__ckLabor===lifecycle&&root.isConnected;
 const visible=()=>{if(!alive())return false;for(let n=root;n&&n.nodeType===1;n=n.parentElement)if(n.hidden||n.style.display==='none'||n.classList.contains('hidden'))return false;return true;};
 await CKSession.ready;
 const config=await loadConfig();if(!alive())return lifecycle;
 const manager=config.user.role==='manager',canEdit=['manager','dispatcher'].includes(config.user.role),manageDaily=!field&&canEdit,removeDaily=!field&&manager;
 root.classList.add('ck-labor');root.classList.toggle('ck-labor-field',field);
 const uid='ck-labor-'+crypto.randomUUID(),departmentOptions=(value='')=>Object.entries(managementLabels).map(([k,v])=>`<option value="${k}" ${k===value?'selected':''}>${v}</option>`).join('');
 root.innerHTML=`<div class="ck-section-heading"><div><p class="ck-eyebrow">PEOPLE & TIME</p><h2>${field?'现场人员与休息 / 인원·휴식':'日当人力 / 일용직 인력'}</h2><p>当天签到、实际休息与部门作业时长</p></div><div class="ck-actions">${!field?'<a href="/attendance/" target="_blank">打开签到点 ↗</a><button data-permanent>长期工牌管理</button><button data-samename>同名人员核实</button><button data-register>登记长期工牌</button><button data-export>导出 CSV</button>':''}</div></div>
 <div class="ck-filters"><label>日期 / 날짜<input type="date" data-date value="${config.day}"></label><label>人力公司 / 인력회사<select data-company><option value="">全部 / 전체</option>${agencies.map(a=>`<option value="${e(a)}">${e(company(a))}</option>`).join('')}</select></label><label>管理部门 / 관리 부서<select data-department-filter><option value="all" selected>全部 / 전체</option>${departmentOptions('all')}</select></label><label>当前作业 / 현재 작업<select data-status-filter><option value="">全部 / 전체</option>${Object.entries(statusLabels).map(([k,v])=>`<option value="${k}">${v}</option>`).join('')}</select></label><label>姓名、工牌、单号或派审员 / 이름·명찰·작업번호·담당자<input data-search placeholder="搜索 / 검색"></label><button data-refresh>刷新 / 새로고침</button></div>
 <p data-error class="ck-error" role="alert" hidden></p><p data-report-freshness class="ck-muted" role="status" aria-live="polite"></p><p data-feedback class="ck-muted" role="status" aria-live="polite"></p><div data-summary class="ck-metrics ck-workforce-metrics"></div><div data-peoplecards class="ck-person-cards"></div>
 <div class="ck-table-wrap" tabindex="0" aria-label="日当签到记录 / 일용직 출근 기록"><table class="ck-table ck-workforce-table"><thead><tr><th>姓名 / 이름</th><th>公司</th><th>管理部门 / 관리 부서</th><th>上下班 / 当前作业</th><th>在岗</th><th>大货</th><th>代发</th><th>进口</th><th>休息</th><th>未归属</th><th>其他 / 冲突</th><th>操作</th></tr></thead><tbody data-rows></tbody></table></div>
 <p class="ck-muted">管理部门为当天安排标记，不改变实际作业或工时。当前未分配仅指已签到、未签退且不在作业或休息中的人员。<br>관리 부서는 당일 표시이며 실제 작업·근무시간은 바뀌지 않습니다. 배정 대기는 출근 중 작업·휴식이 없는 인원입니다.<br>单位：小时:分钟；个人时间不除以人数。仅扣除明确登记的休息。未归属不等于休息或当前空闲。未签退记录计算至本次刷新时间。</p>
 ${removeDaily?'<label class="ck-deleted-toggle"><input type="checkbox" data-show-deleted> 显示已删除签到 / 삭제된 출근 보기 <span data-deleted-count></span></label><section data-deleted hidden aria-label="已删除签到 / 삭제된 출근"></section>':''}<div data-agencies class="ck-agency-totals"></div><div data-stale></div>
 <dialog data-detail-dialog class="ck-attendance-detail" aria-labelledby="${uid}-detail-title"><div data-detail></div></dialog>
 <dialog data-editor class="ck-editor" aria-labelledby="${uid}-editor-title"><form><h2 id="${uid}-editor-title"></h2><div data-fields></div><p role="alert" class="ck-error" hidden></p><p data-pending role="status" class="ck-muted" hidden>正在保存；关闭窗口不会撤销已提交的操作。 / 저장 중입니다. 창을 닫아도 제출된 작업은 취소되지 않습니다.</p><div class="ck-actions"><button type="button" data-cancel>取消 / 취소</button><button type="submit">保存 / 저장</button></div></form></dialog>`;
 const $=s=>root.querySelector('[data-'+s+']'),editor=$('editor'),detailDialog=$('detail-dialog');
 freshness=reportRefresh({root,config,alive,visible,date:()=>$('date').value,paused:()=>mutating||editor.open||detailDialog.open,refresh});
 const showError=x=>{if(alive()){$('error').hidden=false;$('error').textContent=x.message;}};
 const isDaily=r=>r.personType!=='employee'&&!String(r.badgeId||'').startsWith('EMP-');
 const matches=x=>{const r=x.record;return (!$('company').value||r.agency===$('company').value)&&(!$('search').value||(r.name+' '+r.badgeId+' '+[...(x.currentJobs||[]),...(x.segments||[])].map(jobSummary).join(' ')).toLowerCase().includes($('search').value.toLowerCase()))&&($('department-filter').value==='all'||(r.managementDepartment||'')===$('department-filter').value);};
 const filtered=()=>report?report.items.filter(x=>!x.record.voided&&matches(x)&&(!$('status-filter').value||x.status===$('status-filter').value)):[];
 const deleted=()=>removeDaily&&report?(report.deletedItems||[]).filter(matches):[];
 const find=id=>[...(report?.items||[]),...(removeDaily?report?.deletedItems||[]:[])].find(x=>x.record.id===id);
 const rowOrigin=id=>[...root.querySelectorAll('[data-person]')].find(b=>b.dataset.person===id&&!b.closest('[hidden]')&&(!field||b.closest('.ck-person-cards')));
 function focusBack(origin,fallback){if(!visible())return;const target=origin?.isConnected&&!origin.disabled?origin:fallback;target?.focus({preventScroll:true});}
 function closeEditor(){editorVersion++;if(editor.open)editor.close();}
 function closeDetail(){selectedId='';if(detailDialog.open)detailDialog.close();}
 editor.addEventListener('cancel',ev=>{ev.preventDefault();closeEditor();});
 editor.addEventListener('close',()=>{if(editor.open)return;editorVersion++;focusBack(editorOrigin,detailDialog.open?$('close-detail'):$('refresh'));});
 detailDialog.addEventListener('cancel',ev=>{ev.preventDefault();closeDetail();});
 detailDialog.addEventListener('close',()=>{if(detailDialog.open)return;const id=detailDialog.dataset.recordId;selectedId='';focusBack(detailOrigin,rowOrigin(id)||$('refresh'));if(visible()&&detailScroll){window.scrollTo(detailScroll.x,detailScroll.y);const table=root.querySelector('.ck-table-wrap');if(table)table.scrollLeft=detailScroll.table;}detailScroll=null;});
 function leave(){closeEditor();closeDetail();}
 window.addEventListener('pagehide',leave);
 let wasVisible=visible();observer=new MutationObserver(()=>{if(!alive()){lifecycle.destroy();return;}const shown=visible(),returned=shown&&!wasVisible;wasVisible=shown;if(!shown)leave();freshness.check(returned);});
 for(let n=root;n&&n.nodeType===1;n=n.parentElement)observer.observe(n,{attributes:true,attributeFilter:['hidden','style','class'],childList:true});
 function lockActions(){root.querySelectorAll('[data-mutation]').forEach(b=>{b.disabled=mutating||b.dataset.blocked==='true';});}
 async function refresh({background=false}={}){
  if(!alive()||(background&&!freshness.canRead()))return false;
  const version=++readVersion,date=$('date').value;if(!date)return false;
  freshness.begin();if(!background)$('refresh').disabled=true;root.setAttribute('aria-busy','true');
  try{const next=await api('summary',{date,scope:field?'all':'daily'});if(!alive()||version!==readVersion||date!==$('date').value)return false;
   if(background&&!freshness.canApply()){freshness.defer();return false;}
   if(!next||!Array.isArray(next.items))throw Error('作业状态读取失败，请重试 / 작업 상태를 다시 불러오세요');
   report=next;$('error').hidden=true;if(background)preserveReportView(root,draw);else draw();freshness.success(next.asOf);
   if(!background&&selectedId&&detailDialog.open){if(find(selectedId))renderDetail(selectedId);else closeDetail();}return true;
  }catch(x){if(alive()&&version===readVersion){freshness.failure();showError(x);}return false;}
  finally{freshness.end();if(alive()&&version===readVersion){$('refresh').disabled=false;root.removeAttribute('aria-busy');}}
 }
 function draw(){
  if(!alive())return;const items=filtered(),sum=k=>items.reduce((n,x)=>n+(x.totals[k]||0),0);
  $('summary').innerHTML=[['当天签到 / 출근',items.length+' 人'],['尚未签退 / 미퇴근',items.filter(x=>!x.record.outAt).length+' 人'],['当前未分配 / 배정 대기',items.filter(x=>x.status==='unassigned').length+' 人'],['部门作业 / 부서 작업',hours(sum('bulk')+sum('direct_ship')+sum('import'))],['已登记休息 / 등록 휴식',hours(sum('rest'))]].map(([l,v])=>`<article><span>${l}</span><strong>${v}</strong></article>`).join('');
  const mark=x=>isDaily(x.record)?manageDaily?`<button class="ck-small ck-department-button" data-department="${e(x.record.id)}" data-mutation aria-label="${e(x.record.name)}：管理部门 / 관리 부서">${e(managementLabel(x.record.managementDepartment))} <span aria-hidden="true">✎</span></button>`:`<span class="ck-management-label">${e(managementLabel(x.record.managementDepartment))}</span>`:'—';
  $('rows').innerHTML=items.map(x=>{const r=x.record,v=x.totals;return `<tr><td><strong>${e(r.name)}</strong><small>${e(r.badgeId)}</small></td><td>${e(company(r.agency))}</td><td>${mark(x)}</td><td>${at(r.inAt)} – ${at(r.outAt)}${statusMarkup(x)}${v.flags.length?'<small>待核实 / 확인 필요</small>':''}</td>${['presence','bulk','direct_ship','import','rest','unassigned'].map(k=>`<td>${hours(v[k])}</td>`).join('')}<td>${hours(v.other)} / ${hours(v.conflict)}</td><td><div class="ck-row-actions"><button class="ck-small" data-person="${e(r.id)}">明细 / 상세</button>${removeDaily&&isDaily(r)?`<button class="ck-small ck-danger-link" data-void="${e(r.id)}" data-mutation data-blocked="${x.canVoid!==true}" ${x.canVoid!==true?'disabled':''} title="${e(x.voidBlockedReason||'删除误签到 / 잘못된 출근 삭제')}">删除 / 삭제</button>`:''}</div></td></tr>`;}).join('')||`<tr><td colspan="12" class="ck-empty">${report?'当天暂无符合条件的签到记录 / 조건에 맞는 출근 기록이 없습니다':'正在读取 / 불러오는 중'}</td></tr>`;
  $('agencies').innerHTML=agencies.map(a=>{const ar=items.filter(x=>x.record.agency===a);return `<article><h3>${e(company(a))}</h3><p>${ar.length} 人签到 · ${ar.filter(x=>!x.record.outAt).length} 人未签退</p><p>${['bulk','direct_ship','import'].map(k=>dept(k)+' '+hours(ar.reduce((n,x)=>n+x.totals[k],0))).join('　')}</p></article>`;}).join('');
  $('stale').innerHTML=report?.stale?.length?`<p class="ck-warning">此前有 ${report.stale.length}${report.stale.length===100?'＋':''} 条未签退记录，请按对应日期核实补卡，不计入今天出勤。</p>`:'';
  if(field)$('peoplecards').innerHTML=items.map(x=>`<article><div><strong>${e(x.record.name)}</strong><small>${e(company(x.record.agency))}</small></div>${statusMarkup(x)}<p>管理部门 / 관리 부서 ${mark(x)}</p><button class="ck-small" data-person="${e(x.record.id)}">明细与休息 / 상세·휴식</button></article>`).join('')||'<p class="ck-muted">当天暂无符合条件的签到人员 / 조건에 맞는 인원이 없습니다.</p>';
  if(removeDaily){const removed=deleted();$('deleted-count').textContent=`(${removed.length})`;$('deleted').hidden=!$('show-deleted').checked;$('deleted').innerHTML=`<h3>已删除签到 / 삭제된 출근</h3><p class="ck-muted">以下记录不计入人数、工时和导出。恢复会使用原签到记录与工牌。<br>인원·근무시간·내보내기에서 제외됩니다. 복구 시 기존 출근 기록과 명찰을 사용합니다.</p><div class="ck-table-wrap"><table class="ck-table ck-deleted-table"><thead><tr><th>姓名 / 이름</th><th>公司 / 회사</th><th>管理部门 / 관리 부서</th><th>删除说明 / 삭제 사유</th><th>操作 / 작업</th></tr></thead><tbody>${removed.map(x=>{const r=x.record;return `<tr><td><strong>${e(r.name)}</strong><small>${e(r.badgeId)}</small></td><td>${e(company(r.agency))}</td><td>${e(managementLabel(r.managementDepartment))}</td><td>${e(r.voidReason)}<small>${at(r.voidedAt)}</small></td><td><div class="ck-row-actions"><button class="ck-small" data-person="${e(r.id)}">明细 / 상세</button><button class="ck-small" data-restore="${e(r.id)}" data-mutation>恢复 / 복구</button></div></td></tr>`;}).join('')||'<tr><td colspan="5" class="ck-empty">没有符合条件的已删除记录 / 삭제된 기록이 없습니다</td></tr>'}</tbody></table></div>`;}
  bindRows(root);lockActions();
 }
 function bindRows(host){host.querySelectorAll('[data-person]').forEach(b=>b.onclick=()=>openDetail(b.dataset.person,b));host.querySelectorAll('[data-department]').forEach(b=>b.onclick=()=>departmentEditor(find(b.dataset.department)));host.querySelectorAll('[data-void]').forEach(b=>b.onclick=()=>removeEditor(find(b.dataset.void),false));host.querySelectorAll('[data-restore]').forEach(b=>b.onclick=()=>removeEditor(find(b.dataset.restore),true));}
 function dialog(title,html,action,data={},done,values,submitLabel='保存 / 저장'){
  if(mutating||!visible())return;editorVersion++;const token=editorVersion;editorOrigin=document.activeElement;
  const form=editor.querySelector('form'),err=form.querySelector('[role=alert]'),submit=form.querySelector('[type=submit]');
  editor.querySelector('h2').textContent=title;$('fields').innerHTML=html;err.hidden=true;$('pending').hidden=true;submit.hidden=false;submit.disabled=false;submit.textContent=submitLabel;submit.classList.toggle('ck-danger',action==='void');$('cancel').textContent='取消 / 취소';
  let pending=false;
  form.onsubmit=async ev=>{ev.preventDefault();if(pending||mutating||token!==editorVersion||!editor.open)return;
   const body=values?values(new FormData(form)):{...data,...Object.fromEntries(new FormData(form))},signature=action+JSON.stringify(body);
   if(!retries.has(signature))retries.set(signature,crypto.randomUUID());pending=true;mutating=true;lockActions();err.hidden=true;$('pending').hidden=false;
   const controls=[...form.querySelectorAll('input,select,textarea,[type=submit]')].map(x=>[x,x.disabled]);controls.forEach(([x])=>x.disabled=true);
   try{const result=await api(action,{...body,client_req_id:retries.get(signature)});retries.delete(signature);
    const current=alive()&&token===editorVersion&&editor.open&&visible();if(current)closeEditor();
    if(alive()){const updated=await refresh();if(updated)$('feedback').textContent=action==='void'?'已删除误签到，可在已删除记录中恢复。 / 삭제되었습니다. 삭제된 기록에서 복구할 수 있습니다.':action==='restore'?'已恢复原签到记录。 / 기존 출근 기록을 복구했습니다.':'已保存 / 저장되었습니다';}
    if(current&&visible()&&!editor.open){focusBack(editorOrigin,detailDialog.open?$('close-detail'):rowOrigin(body.id)||$('refresh'));if(done)await done(result);}
   }catch(x){if(x.businessError)retries.delete(signature);if(alive()&&token===editorVersion&&editor.open){err.hidden=false;err.textContent=x.message;}else showError(x);}
   finally{pending=false;mutating=false;freshness.check();if(alive()){lockActions();if(token===editorVersion&&editor.open){$('pending').hidden=true;controls.forEach(([x,disabled])=>x.disabled=disabled);}}}
  };
  $('cancel').onclick=closeEditor;editor.showModal();
 }
 function departmentEditor(x){if(!x||!manageDaily||!isDaily(x.record)||x.record.voided)return;const r=x.record;dialog('标记当天管理部门 / 당일 관리 부서',`<p><strong>${e(r.name)}</strong> · ${e(r.day)}</p><p class="ck-muted">仅标记当天安排，不修改人员所属部门、任务归属或已记录工时。<br>당일 배치 표시만 변경하며 소속 부서·실제 작업·근무시간은 바뀌지 않습니다.</p><label>管理部门 / 관리 부서<select name="department">${departmentOptions(r.managementDepartment)}</select></label>`,'department',{id:r.id,version:r.version});}
 function removeEditor(x,restore){if(!x||!removeDaily||!isDaily(x.record)||(!restore&&x.canVoid!==true))return;const r=x.record;dialog(restore?'恢复签到 / 출근 복구':'删除误签到 / 잘못된 출근 삭제',`<p><strong>${e(r.name)}</strong> · ${e(r.day)} · ${e(r.badgeId)}</p><p class="${restore?'ck-muted':'ck-warning'}">${restore?'恢复原签到时间与工牌，重新计入当天人数和工时。不会新建签到，也不会恢复作业任务。<br>기존 출근 시간과 명찰을 복구하고 인원·근무시간에 다시 포함합니다. 작업은 재개하지 않습니다.':'仅用于误签到或重复签到。删除后不计入当天人数、工时和导出，可从已删除记录恢复。不会关闭任何作业；有作业、休息或借调记录的签到不能删除。<br>잘못되거나 중복된 출근만 삭제하세요. 인원·근무시간·내보내기에서 제외되며 복구할 수 있습니다. 작업을 종료하지 않으며 작업·휴식·지원 기록이 있으면 삭제할 수 없습니다.'}</p><label>${restore?'恢复原因 / 복구 사유':'删除原因 / 삭제 사유'}<textarea name="reason" required maxlength="500"></textarea></label>`,restore?'restore':'void',{id:r.id,version:r.version},null,null,restore?'确认恢复 / 복구 확인':'确认删除 / 삭제 확인');}
 function openDetail(id,origin){if(!find(id)||!visible())return;selectedId=id;detailOrigin=origin||document.activeElement;detailScroll={x:window.scrollX,y:window.scrollY,table:root.querySelector('.ck-table-wrap')?.scrollLeft||0};renderDetail(id);if(!detailDialog.open)detailDialog.showModal();$('close-detail').focus({preventScroll:true});}
 function renderDetail(id){
  const x=find(id);if(!x)return;const r=x.record,today=r.day===freshness.today(),scroll=detailDialog.scrollTop,focused=document.activeElement?.dataset;
  const events=[...(x.segments||[]).map(s=>({start:s.start,end:s.end,label:jobContext(s,true)+(s.reason?' · '+window.CKDocumentLabels.jobLeaveReason(s.reason):'')})),...(x.breaks||[]).map(b=>({...b,label:'实际休息 / 휴식'}))].sort((a,b)=>a.start.localeCompare(b.start));
  detailDialog.dataset.recordId=id;
  $('detail').innerHTML=`<div class="ck-detail-heading"><div><h2 id="${uid}-detail-title">${e(r.name)} · 当日明细 / 일별 상세</h2><p class="ck-muted">${e(r.day)} · ${e(r.badgeId)} · ${e(company(r.agency))}</p></div><button type="button" data-close-detail class="ck-small" aria-label="关闭明细 / 상세 닫기">关闭 / 닫기</button></div>
  ${r.voided?`<p class="ck-warning">已删除，不计入统计 / 삭제됨 · 집계 제외<br>${e(r.voidReason)} · ${at(r.voidedAt)}</p>`:statusMarkup(x)}
  ${x.totals.flags.length?`<p class="ck-warning">${x.totals.flags.map(e).join('；')}</p>`:''}<p>上班 / 출근 ${at(r.inAt)} · 下班 / 퇴근 ${at(r.outAt)}</p>
  ${isDaily(r)?`<p>管理部门 / 관리 부서：<strong>${e(managementLabel(r.managementDepartment))}</strong></p>`:''}<div class="ck-actions">
  ${!r.voided&&canEdit&&today&&!r.outAt?`<button data-rest data-mutation>${x.status==='rest'?'结束休息 / 휴식 종료':'开始休息 / 휴식 시작'}</button>`:''}
  ${!r.voided&&manager?'<button data-correct data-mutation>核实补卡 / 기록 정정</button>':''}
  ${!r.voided&&manageDaily&&isDaily(r)?`<button data-department="${e(id)}" data-mutation>标记部门 / 부서 표시</button>`:''}
  ${!r.voided&&today&&!field?'<button data-reprint data-mutation>补打工牌 / 재출력</button>':''}
  ${removeDaily&&isDaily(r)?r.voided?`<button data-restore="${e(id)}" data-mutation>恢复签到 / 출근 복구</button>`:`<button class="ck-danger-link" data-void="${e(id)}" data-mutation data-blocked="${x.canVoid!==true}" ${x.canVoid!==true?'disabled':''}>删除误签到 / 잘못된 출근 삭제</button>`:''}</div>
  ${removeDaily&&isDaily(r)&&!r.voided&&x.canVoid!==true?`<p class="ck-muted">无法删除 / 삭제 불가：${e(x.voidBlockedReason||'请刷新后核实记录 / 새로고침 후 기록을 확인하세요')}</p>`:''}
  <h3>作业与休息 / 작업·휴식</h3><ol class="ck-timeline">${events.map(s=>`<li><time>${at(s.start)} – ${at(s.end)}</time><span>${e(s.label)}</span></li>`).join('')||'<li>暂无作业或休息记录 / 작업·휴식 기록이 없습니다</li>'}</ol>
  <details><summary>修改与打印历史 / 변경·출력 이력 · ${(x.events||[]).length} 条</summary>${(x.events||[]).map(a=>{const before=a.before||{},after=a.after||{},label={department:'管理部门 / 관리 부서',void:'删除签到 / 출근 삭제',restore:'恢复签到 / 출근 복구'}[a.action.replace('sop_attendance_','')]||a.action.replace('sop_attendance_','');return `<p>${at(a.at)} · ${e(label)} · ${e(a.actorName||a.actor)}${before.agency!==after.agency?' · '+e(before.agency||'')+' → '+e(after.agency||''):''}${before.management_department!==after.management_department?' · '+e(managementLabel(before.management_department))+' → '+e(managementLabel(after.management_department)):''}${after.correction_reason||after.void_reason||after.restore_reason?' · '+e(after.correction_reason||after.void_reason||after.restore_reason):''}</p>`;}).join('')}</details>`;
  $('close-detail').onclick=closeDetail;bindRows($('detail'));lockActions();detailDialog.scrollTop=scroll;
  if(focused&&detailDialog.open&&!editor.open){for(const key of ['rest','correct','reprint','closeDetail','department','void','restore'])if(key in focused){const button=$('detail').querySelector('[data-'+key.replace(/[A-Z]/g,c=>'-'+c.toLowerCase())+']');(button&&!button.disabled?button:$('close-detail'))?.focus({preventScroll:true});break;}}
  if($('rest'))$('rest').onclick=()=>dialog(x.status==='rest'?'结束休息 / 휴식 종료':'开始休息 / 휴식 시작',`<p>${e(r.name)}：${x.status==='rest'?'恢复仍在进行的原任务；若原任务已结束，则回到未分配状态。 / 진행 중인 기존 작업만 재개합니다. 종료된 작업이면 배정 대기로 돌아갑니다.':'现在停止个人作业计时，并开始记录休息。 / 개인 작업 시간을 멈추고 휴식을 기록합니다.'}</p>`,x.status==='rest'?'break_end':'break_start',{id:r.id});
  if($('reprint'))$('reprint').onclick=async()=>{if(mutating)return;mutating=true;lockActions();try{await print(r);await refresh();}catch(x){showError(x);}finally{mutating=false;freshness.check();if(alive())lockActions();}};
  if($('correct'))$('correct').onclick=()=>{const local=t=>t?new Date(Date.parse(t)+9*3600000).toISOString().slice(0,16):'';dialog('核实上下班时间 / 출퇴근 정정',`<label>上班（韩国时间） / 출근 (한국 시간)<input name="inAt" type="datetime-local" value="${local(r.inAt)}" required></label><label>下班（留空表示未签退） / 퇴근 (미퇴근 시 비워 두세요)<input name="outAt" type="datetime-local" value="${local(r.outAt)}"></label><label>核实原因 / 정정 사유<textarea name="reason" required maxlength="500"></textarea></label>`,'correct',{},null,fd=>({id:r.id,version:r.version,reason:fd.get('reason'),inAt:fd.get('inAt')+':00+09:00',outAt:fd.get('outAt')?fd.get('outAt')+':00+09:00':''}));};
 }
 if($('samename'))$('samename').onclick=()=>dialog('同名不同人 · 核实后另建签到',`<p>确认是不同的两个人后才使用此入口。新工牌不会覆盖已有签到。</p><label>姓名<input name="name" required maxlength="40"></label><label>人力公司<select name="agency" required><option value="">请选择</option>${agencies.map(a=>`<option value="${e(a)}">${e(company(a))}</option>`).join('')}</select></label><label>核实说明<textarea name="reason" required></textarea></label>`,'checkin',{confirm_distinct_person:true},async r=>{await print(r.record);});
 if($('permanent'))$('permanent').onclick=async()=>{
  if(mutating)return;const version=++editorVersion,button=$('permanent');button.disabled=true;
  try{const result=await api('people');if(!visible()||version!==editorVersion)return;
   dialog('长期工牌 / 고정 명찰',result.items.map(p=>`<p><strong>${e(p.name)}</strong> · ${e(p.badgeId)} <button type="button" data-print-person="${e(p.id)}">补打 / 재출력</button></p>`).join('')||'<p>暂无长期工牌，请先登记。</p>','');
   editor.querySelector('[type=submit]').hidden=true;editor.querySelector('form').onsubmit=ev=>ev.preventDefault();$('cancel').textContent='关闭 / 닫기';
   editor.querySelectorAll('[data-print-person]').forEach(b=>b.onclick=async()=>{if(mutating)return;mutating=true;b.disabled=true;try{closeEditor();await print(result.items.find(p=>p.id===b.dataset.printPerson));}catch(x){showError(x);}finally{mutating=false;freshness.check();if(alive())lockActions();}});
  }catch(x){if(version===editorVersion)showError(x);}finally{if(alive())button.disabled=false;}
 };
 if($('register'))$('register').onclick=()=>dialog('登记长期工牌 / 고정 명찰 등록',`<label>姓名 / 이름<input name="name" maxlength="40" required></label><label>人力公司<select name="agency" required><option value="">请选择 / 선택</option>${agencies.map(a=>`<option value="${e(a)}">${e(company(a))}</option>`).join('')}</select></label><label>已有固定工号（可留空自动生成）<input name="badge" placeholder="DAF-..."></label>`,'register',{},async r=>{await print(r.person);});
 if($('export'))$('export').onclick=()=>{if(!report)return;const cell=v=>{let s=String(v??'');if(/^[\s]*[=+\-@]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};const data=[['日期','姓名','工牌','公司','上班','下班','在岗分钟','大货分钟','代发分钟','进口分钟','休息分钟','未归属分钟','其他分钟','冲突分钟','异常','截至时间','管理部门','当前作业状态','当前作业','派审员 / 배정·검수 담당자'],...filtered().map(({record:r,totals:v,...x})=>[r.day,r.name,r.badgeId,r.agency,r.inAt,r.outAt,...['presence','bulk','direct_ship','import','rest','unassigned','other','conflict'].map(k=>Math.round(v[k]*100)/100),v.flags.join('；'),report.asOf,managementLabel(r.managementDepartment),statusLabels[x.status]||'',(x.currentJobs||[]).map(j=>(j.has_business_reference===false?'':jobDepartment(j.department)+' · ')+window.CKDocumentLabels.jobLabel(j)).join('；'),(x.currentJobs||[]).map(j=>window.CKDocumentLabels.jobDispatcher(j)).join('；')])];const url=URL.createObjectURL(new Blob(['\ufeff'+data.map(row=>row.map(cell).join(',')).join('\r\n')],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='CK_日当人力_'+report.date+'.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
 $('date').onchange=()=>{closeEditor();closeDetail();report=null;freshness.reset();$('feedback').textContent='';draw();refresh();};$('refresh').onclick=refresh;['company','department-filter','status-filter'].forEach(s=>$(s).onchange=draw);$('search').oninput=draw;if(removeDaily)$('show-deleted').onchange=draw;
 draw();await refresh();return {...lifecycle,refresh};
};
})();
