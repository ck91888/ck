(function(){
'use strict';
if(!window.CK_SOP_ROLLOUT?.staging)return;
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const today=()=>new Date(Date.now()+9*3600000).toISOString().slice(0,10);
const time=v=>new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Seoul',dateStyle:'short',timeStyle:'medium'}).format(new Date(v));
const allowed=u=>u&&['manager','dispatcher'].includes(u.role)&&u.scope!=='kiosk'&&(u.role==='manager'||(u.departments||[]).includes('import')&&(!u.daily_dispatch||u.can_dispatch&&(u.dispatch_departments||[]).includes('import')));
const request=async(action,data)=>{let timer;try{return await Promise.race([CKSession.request('sop_import_daily_scan_'+action,data),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('响应超时，结果待核实；请保留输入并重试或读取最新值 / 응답 시간 초과. 입력을 유지하고 다시 조회하세요')),20000);})]);}finally{clearTimeout(timer);}};
const bilingual=(zh,ko)=>'<span>'+zh+'</span><small lang="ko">'+ko+'</small>';
function install(){
 const menu=document.querySelector('#page-import_menu');if(!menu||!window.CKSession)return;
 const entry=document.createElement('button');entry.id='ck-import-daily-entry';entry.type='button';entry.className='btn btn-primary';entry.innerHTML=bilingual('当日扫描结果','당일 스캔 결과');entry.hidden=true;menu.lastElementChild.append(entry);
 const page=document.createElement('div');page.id='page-import_daily_scan';page.className='page';
 page.innerHTML='<div class="topbar"><button class="back-btn" data-back>← 返回 / 뒤로</button><div class="page-title">'+bilingual('当日扫描结果','당일 스캔 결과')+'</div></div><div class="ck-daily-scan">'+
 '<section class="card"><p class="ck-scan-intro">每日下班录入，按韩国业务日期保存。<br><span lang="ko">퇴근 시 한국 업무 날짜별로 입력하세요.</span></p><form novalidate><div class="ck-scan-fields"><label>'+bilingual('业务日期（韩国 UTC+9）','업무 날짜 (한국 UTC+9)')+'<input name="date" type="date" required></label><label>'+bilingual('当天扫描总量','당일 스캔 총수량')+'<input name="total" type="text" inputmode="numeric" autocomplete="off" placeholder="未填 / 미입력" required><small>0 表示确认为零，空白表示未填。<br><span lang="ko">0은 확인된 0이며 빈칸은 미입력입니다.</span></small></label></div><div class="ck-scan-actions"><button type="submit" class="btn btn-primary" data-save disabled>'+bilingual('保存结果','결과 저장')+'</button><button type="button" class="btn btn-outline" data-refresh>'+bilingual('读取最新值','최신 값 조회')+'</button></div><p role="status" aria-live="polite" data-status></p></form><div data-current></div><details class="ck-scan-history"><summary>更正记录 / 변경 이력</summary><ol data-events></ol><button type="button" class="btn btn-outline" data-more hidden>更早记录 / 이전 기록</button></details></section>'+
 '<section class="card ck-scan-metric"><h3>'+bilingual('扫描作业效率','스캔 작업 효율')+'</h3><strong>—</strong><p>待确认统计口径与工时完整性。<br><span lang="ko">집계 기준과 작업시간 기록의 완전성을 확인해야 합니다.</span></p><details><summary>统计说明 / 집계 안내</summary><p>日报总量独立保存，不叠加到每任务产出。数量单位、扫描作业人时或进口组作业人时尚待确认，当前不计算人效。<br><span lang="ko">일일 총수량은 작업별 실적과 별도로 저장합니다. 수량 단위와 작업시간 범위가 확인될 때까지 효율을 계산하지 않습니다.</span></p></details></section>'+
 '<section class="card"><h3>'+bilingual('最近已录入日期','최근 입력 날짜')+'</h3><p class="muted">最近30个日期；其他日期可在上方选择。<br><span lang="ko">최근 30일 기록. 다른 날짜는 위에서 선택하세요.</span></p><div data-recent></div></section></div>';
 document.body.append(page);
 const find=s=>page.querySelector(s),form=find('form'),date=form.elements.date,total=form.elements.total,status=find('[data-status]'),save=find('[data-save]'),refresh=find('[data-refresh]'),more=find('[data-more]');
 let selected='',sequence=0,busy=false,next=null,events=[],drafts=new Map();
 const draft=()=>{if(!drafts.has(selected))drafts.set(selected,{raw:'',dirty:false,version:null,request:null});return drafts.get(selected);};
 const message=(text,bad=false)=>{status.textContent=text;status.classList.toggle('ck-scan-error',bad);};
 const state=()=>{const d=draft();save.disabled=busy||d.version===null||!allowed(CKSession.user);refresh.disabled=busy;date.disabled=busy;total.disabled=busy;more.disabled=busy;};
 const showCurrent=item=>{
  find('[data-current]').innerHTML=item?'<div class="ck-scan-saved"><span>'+bilingual('已保存总量','저장된 총수량')+'</span><strong>'+item.total.toLocaleString()+'</strong><dl><div><dt>记录人 / 입력자</dt><dd>'+esc(item.created_name)+'</dd></div><div><dt>更新人 / 수정자</dt><dd>'+esc(item.updated_name)+'</dd></div><div><dt>更新时间（韩国） / 수정 시간 (한국)</dt><dd>'+esc(time(item.updated_at))+' · v'+item.version+'</dd></div></dl></div>':'<p class="ck-scan-empty">该日期未录入 / 이 날짜는 미입력입니다.</p>';
  save.innerHTML=bilingual(item?'保存更正':'保存结果',item?'수정 저장':'결과 저장');
 };
 const showEvents=()=>{
  find('[data-events]').innerHTML=events.map(e=>'<li><b>'+esc(e.before_total===null?'首次录入 / 최초 입력':e.before_total.toLocaleString())+' → '+e.after_total.toLocaleString()+'</b><span>'+esc(e.actor_name)+' · '+esc(time(e.created_at))+' · v'+e.version+'</span></li>').join('')||'<li>暂无记录 / 기록 없음</li>';
  more.hidden=!next;
 };
 const showRecent=items=>{
  find('[data-recent]').innerHTML=items.map(x=>'<button type="button" class="ck-scan-recent" data-date="'+esc(x.business_date)+'"><span>'+esc(x.business_date)+'<small>'+esc(x.updated_name)+'</small></span><b>'+x.total.toLocaleString()+'</b></button>').join('')||'<p>暂无记录 / 기록 없음</p>';
  find('[data-recent]').querySelectorAll('button').forEach(b=>b.onclick=()=>{if(!busy)openDate(b.dataset.date);});
 };
 async function load(){
  const token=++sequence,key=selected,d=draft();d.version=null;busy=true;state();message('正在读取 / 조회 중');
  try{
   const r=await request('get',{business_date:key});if(token!==sequence||selected!==key)return;
   if(r.business_date!==key||r.item&&r.item.business_date!==key)throw Error('日期响应不一致 / 조회 날짜가 일치하지 않습니다');
   d.version=r.item?.version||0;d.request=null;
   if(!d.dirty)d.raw=r.item?String(r.item.total):'';
   total.value=d.raw;showCurrent(r.item);events=r.events;next=r.next_before_version;showEvents();showRecent(r.recent);
   message(d.dirty?'输入已保留，请与已保存总量核对后再保存 / 입력을 유지했습니다. 저장된 값과 비교 후 저장하세요':'');
  }catch(e){if(token===sequence)message('查询失败，输入已保留 / 조회 실패, 입력 유지\n'+e.message,true);}
  finally{if(token===sequence){busy=false;state();}}
 }
 function openDate(value){selected=value;date.value=value;total.value=draft().raw;find('[data-current]').replaceChildren();find('[data-events]').replaceChildren();more.hidden=true;load();}
 total.oninput=()=>{const d=draft();d.raw=total.value;d.dirty=true;d.request=null;};
 date.onchange=()=>openDate(date.value);
 refresh.onclick=()=>load();
 more.onclick=async()=>{
  if(!next||busy)return;const token=sequence,key=selected;busy=true;state();
  try{const r=await request('get',{business_date:key,before_version:next});if(token!==sequence)return;events.push(...r.events);next=r.next_before_version;showEvents();}
  catch(e){if(token===sequence)message(e.message,true);}finally{if(token===sequence){busy=false;state();}}
 };
 form.onsubmit=async e=>{
  e.preventDefault();if(busy||draft().version===null)return;
  const d=draft(),raw=total.value.trim();
  if(!/^\d+$/.test(raw)||!Number.isSafeInteger(Number(raw))){message('请填写非负整数；空白不是0 / 0 이상의 정수를 입력하세요. 빈칸은 0이 아닙니다.',true);return;}
  if(!date.value){message('请选择业务日期 / 업무 날짜를 선택하세요',true);return;}
  const key=selected,token=++sequence;d.request=d.request||crypto.randomUUID();busy=true;state();message('正在保存 / 저장 중');
  try{
   const r=await request('save',{business_date:key,total:Number(raw),version:d.version,client_req_id:d.request});
   if(token!==sequence||key!==selected)return;
   d.version=r.item.version;d.raw=String(r.item.total);d.dirty=false;d.request=null;total.value=d.raw;showCurrent(r.item);
   message(r.saved_version<r.item.version?'此前保存已确认；该日已有后续更正，现显示最新值 / 이전 저장 확인. 이후 수정된 최신 값을 표시합니다.':'已保存 / 저장 완료');
   // A failed history refresh must not turn a successful save into a false failure.
   try{const latest=await request('get',{business_date:key});if(token!==sequence)return;events=latest.events;next=latest.next_before_version;showEvents();showRecent(latest.recent);}
   catch{if(token===sequence)message('已保存；记录列表暂未刷新，可读取最新值 / 저장 완료. 목록을 새로 조회하세요');}
  }catch(error){if(token===sequence)message(error.message+'\n输入已保留；可重试，或读取最新值后核对更正 / 입력 유지. 재시도하거나 최신 값과 비교하세요.',true);}
  finally{if(token===sequence){busy=false;state();}}
 };
 find('[data-back]').onclick=()=>goPage('import_menu');
 entry.onclick=()=>{showPage('import_daily_scan');openDate(selected||today());};
 CKSession.ready.then(u=>{entry.hidden=!allowed(u);});
 window.addEventListener('ck-field-session-change',()=>{sequence++;drafts.clear();selected='';total.value='';find('[data-current]').replaceChildren();find('[data-recent]').replaceChildren();find('[data-events]').replaceChildren();entry.hidden=true;save.disabled=true;});
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',install);else install();
})();
