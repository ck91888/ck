(function(){
 'use strict';
 if(location.pathname.split('/')[1]!=='002'||!window.CK_SOP_ROLLOUT?.staging||!window.CKSession)return;
 CKSession.ready.then(()=>{
  if(CKSession.scope!=='office'||!['manager','service','reviewer'].includes(CKSession.user?.role))return;
  const tab=document.createElement('button');tab.type='button';tab.dataset.tab='office_duty';tab.textContent='订单处理';document.getElementById('mainTabs').append(tab);
  const root=document.createElement('section');root.id='view-office_duty';root.className='view';root.style.display='none';document.getElementById('view-home').after(root);
  root.innerHTML='<div class="card"><h2>订单处理 / 주문 처리</h2><p>只记录实际值班工时；接单、审单、制单、改单和交接合并记录。 / 주문 처리 근무시간만 기록합니다.</p><form data-duty-search><label>实际职员：姓名、工号或工牌 / 이름·사번·명찰<input data-duty-term autocomplete="off" required maxlength="100"></label><button class="btn btn-outline" type="submit">查找职员 / 직원 찾기</button></form><div data-duty-people></div><p role="alert" data-duty-error></p><div data-duty-state aria-live="polite"></div><div data-duty-actions class="toolbar"></div></div>';
  const $=s=>root.querySelector('[data-duty-'+s+']'),key='ck-office-duty:'+CKSession.user.id;
  let selected='',state=null,busy=false,pending=null,anchor=0,generation=0;
  try{const saved=JSON.parse(sessionStorage.getItem(key)||'{}');selected=saved.selected||'';pending=saved.pending||null;}catch{}
  const save=()=>sessionStorage.setItem(key,JSON.stringify({selected,pending}));
  const request=async(action,data)=>{let timer;try{return await Promise.race([CKSession.request(action,data),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('响应尚未确认，请刷新状态或重试原操作 / 응답 미확인·상태 확인 또는 재시도')),20000);})]);}finally{clearTimeout(timer);}};
  const button=(label,fn,disabled=false)=>{const b=document.createElement('button');b.type='button';b.className='btn btn-outline';b.textContent=label;b.disabled=disabled||busy;b.onclick=fn;$('actions').append(b);};
  function render(){
   $('state').replaceChildren();$('actions').replaceChildren();
   if(state){const p=document.createElement('p');p.textContent=state.person.name+' · '+state.person.badgeId+' · '+state.person.department_label;$('state').append(p);
    const status=document.createElement('p'),j=state.job;status.textContent=j?.borrowed?'现场借调中，办公室计时已停止 / 현장 지원 중':({working:'值班中 / 근무 중',system_closed:'系统04:00强制结束 · 待核实 / 시스템 종료·확인 필요',paused:'已暂停 / 중지',awaiting_close:'计时已停止，请核对后继续 / 중지·확인 후 재개',completed:'已结束 / 종료'})[j?.status]||'尚未开始 / 시작 전';$('state').append(status);
    const time=document.createElement('p');time.dataset.dutyClock='';$('state').append(time);tick();
    if(!pending&&!j?.borrowed){if(!j||['completed','cancelled'].includes(j.status))button('开始值班 / 근무 시작',()=>mutate('start'));
     if(j?.status==='working')button('暂停 / 중지',()=>mutate('pause'));
     if(['paused','system_closed','awaiting_close'].includes(j?.status))button('继续 / 재개',()=>mutate('resume'));
     if(j&&!['completed','cancelled'].includes(j.status))button('结束值班 / 근무 종료',()=>mutate('finish'));}
   }
   if(pending)button('重试未确认操作 / 미확인 작업 재시도',()=>submit());
   if(selected)button('刷新服务器状态 / 상태 새로고침',()=>load());
  }
  function tick(){const el=$('clock');if(!el||!state)return;const running=state.job?.segments.some(s=>!s.left_at),seconds=(state.job?.seconds||0)+(running?Math.max(0,(performance.now()-anchor)/1000):0);el.textContent=(state.job?.needs_review?'含系统强制结束待核实时段，未计入下列工时 / 확인 필요 · ':'')+'已记录 '+(seconds/3600).toFixed(2)+' 小时 / 기록 시간（显示截至最近服务器核对；报表按实际时段统计）';}
  async function load(){const token=++generation;if(!selected)return;busy=true;render();$('error').textContent='';try{const result=await request('sop_office_duty_get',{badge:selected});if(token===generation){state=result;anchor=performance.now();}}catch(e){if(token===generation)$('error').textContent=e.message;}finally{if(token===generation){busy=false;render();}}}
  function mutate(kind){if(busy||pending||!state)return;pending={action:'sop_office_duty_'+kind,badge:selected,client_req_id:crypto.randomUUID(),...(kind==='start'?{}:{job_id:state.job.id,revision:state.job.revision})};save();submit();}
  async function submit(){if(busy||!pending)return;busy=true;render();$('error').textContent='';const intent=pending;try{const result=await request(intent.action,intent);state=result;anchor=performance.now();pending=null;save();}catch(e){if(e.httpStatus>=400&&e.httpStatus<500&&!e.operatorContextChanged){pending=null;save();} $('error').textContent=e.message;}finally{busy=false;render();}}
  $('search').onsubmit=async e=>{e.preventDefault();if(busy||pending){$('error').textContent='请先核对或重试未确认操作';return;}busy=true;render();$('error').textContent='';try{const r=await request('sop_attendance_employee_search',{term:$('term').value});$('people').replaceChildren();if(!r.items.length)$('error').textContent='没有找到已登记职员 / 등록된 직원 없음';for(const p of r.items){const b=document.createElement('button');b.type='button';b.className='btn btn-outline';b.textContent=p.name+' · '+p.employeeNo;b.onclick=()=>{if(busy||pending)return;selected=p.badgeId;state=null;save();load();};$('people').append(b);}}catch(x){$('error').textContent=x.message;}finally{busy=false;render();}};
  const go=window.goTab;window.goTab=function(name,...args){const result=go.call(this,name,...args);if(name==='office_duty')load();return result;};tab.onclick=()=>window.goTab('office_duty',tab);
  setInterval(tick,1000);if(selected)load();else render();
 });
})();
