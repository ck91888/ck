/* Exact permanent identities own grants; a temporary badge needs an explicit verified link. */
(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const depts={bulk:'大货 / 대량',direct_ship:'代发 / 출고대행',import:'进口 / 수입'},labels=ds=>ds.length?ds.map(d=>depts[d]||d).join(' · '):'未选择 / 선택 없음';
const time=s=>s?new Date(s).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false})+' KST':'—';
window.CKDailyDispatchPermissions=async function(root,origin){
 if(CKSession.user?.role!=='manager'||CKSession.scope!=='office')return;
 root.querySelector('[data-dispatch-dialog]')?.remove();
 const d=document.createElement('dialog');d.className='ck-dispatch-permissions';d.dataset.dispatchDialog='';d.setAttribute('aria-label','派工权限 / 작업 배정 권한');root.append(d);
 let items=[],busy=false,epoch=0;const retries=new Map();
 const request=(action,data={})=>CKSession.request('sop_daily_dispatch_'+action,data);
 const close=()=>{if(busy)return;epoch++;d.close();d.remove();origin?.focus({preventScroll:true});};
 d.oncancel=e=>{e.preventDefault();close();};
 const status=p=>!p.enabled?'工牌停用 / 명찰 비활성':p.grant.enabled?'已开启 / 허용':p.grant.version?'已撤销 / 해제':'未开启 / 미허용';
 const identity=p=>`<div class="dp-identity"><strong>${esc(p.name)}</strong><span>${esc(p.badge)}</span><small>${esc(p.agency)}</small></div>`;
 function frame(title,content,back){epoch++;d.innerHTML=`<header><h2>${title}</h2><button type="button" data-close>关闭 / 닫기</button></header>${content}<p data-error role="alert" hidden></p>${back?'<button type="button" data-back>取消，返回列表 / 취소·목록</button>':''}`;d.querySelector('[data-close]').onclick=close;if(back)d.querySelector('[data-back]').onclick=()=>list();d.scrollTop=0;}
 function error(e){if(d.isConnected){const n=d.querySelector('[data-error]');n.hidden=false;n.textContent=e.message||'网络暂不可用，请重试 / 네트워크 연결 후 다시 시도하세요';}}
 async function save(action,data){
  if(busy)return;busy=true;const key=JSON.stringify({action,...data});if(!retries.has(key))retries.set(key,crypto.randomUUID());
  d.querySelectorAll('button,input,textarea').forEach(n=>n.disabled=true);
  try{await request(action,{...data,client_req_id:retries.get(key)});retries.delete(key);await list('已保存 / 저장되었습니다');}catch(e){error(e);}finally{busy=false;d.querySelectorAll('button,input,textarea').forEach(n=>n.disabled=n.dataset.blocked==='true');}
 }
 async function list(message=''){
  frame('派工权限 <small>작업 배정 권한</small>',`<p class="dp-note">授权保存在本人长期档案，默认关闭；开启后持续有效，直到管理员手动撤销。每次派工仍需当天有效签到。<br>본인 고정 기록에 권한을 저장하며, 관리자가 해제할 때까지 유지됩니다. 배정 시 당일 출근이 필요합니다.</p><p role="status">${esc(message)}</p><label>姓名或长期工牌 / 이름·고정 명찰<input data-search type="search" placeholder="DAF-… / 姓名" autocomplete="off"></label><div data-list>正在读取 / 불러오는 중</div><button type="button" data-reload>刷新 / 새로고침</button>`);
  const version=epoch;d.querySelector('[data-reload]').onclick=()=>list();
  try{const r=await request('list');if(version!==epoch||!d.isConnected)return;items=r.items;const draw=()=>{const term=d.querySelector('[data-search]').value.trim().toLowerCase();d.querySelector('[data-list]').innerHTML=items.filter(p=>(p.name+' '+p.badge).toLowerCase().includes(term)).map(p=>`<article data-person="${esc(p.id)}">${identity(p)}<div class="dp-state"><strong>${status(p)}</strong><span>用工部门 / 인력 부서</span><span>${esc(labels(p.grant.departments))}</span>${!p.enabled&&p.grant.enabled?'<small>授权保留；恢复工牌并有效签到后可派工 / 명찰 복구·출근 후 배정 가능</small>':''}<small>${p.grant.updatedAt?esc(p.grant.updatedName)+' · '+esc(time(p.grant.updatedAt)):'尚无授权记录 / 권한 기록 없음'}</small></div><div class="dp-actions"><button type="button" data-edit="${esc(p.id)}" ${!p.enabled&&!p.grant.enabled?'disabled data-blocked="true"':''}>${p.grant.enabled?'调整 / 변경':'开启 / 허용'}</button>${p.grant.enabled?`<button type="button" data-revoke="${esc(p.id)}">撤销 / 해제</button>`:''}<button type="button" data-link="${esc(p.id)}">关联临时牌 / 임시 명찰 연결</button><button type="button" data-history="${esc(p.id)}">记录 / 이력</button></div></article>`).join('')||'<p class="dp-empty">暂无符合条件的长期档案，请先登记长期工牌。<br>일치하는 고정 기록이 없습니다.</p>';for(const action of ['edit','revoke','link','history'])d.querySelectorAll('[data-'+action+']').forEach(b=>b.onclick=()=>{const p=items.find(x=>x.id===b.dataset[action]);({edit:()=>edit(p),revoke:()=>edit(p,true),link:()=>link(p),history:()=>history(p)})[action]();});};d.querySelector('[data-search]').oninput=draw;draw();
  }catch(e){if(version===epoch){d.querySelector('[data-list]').textContent='读取失败，请刷新 / 불러오지 못했습니다';error(e);}}
 }
 function edit(p,revoke=false){
  frame(revoke?'撤销派工权限 <small>배정 권한 해제</small>':'开启 / 调整权限 <small>권한 허용·변경</small>',`${identity(p)}<p>当前 / 현재：${status(p)}<br>用工部门 / 인력 부서：${esc(labels(p.grant.departments))}</p><form>${revoke?'<p class="dp-note">立即停止新派工；已有任务仍可正常收尾。<br>새 배정은 즉시 중단되며 기존 작업은 마무리할 수 있습니다.</p>':`<fieldset><legend>明确选择用工部门 / 인력 부서 선택</legend>${Object.entries(depts).map(([k,v])=>`<label class="dp-check"><input type="checkbox" name="department" value="${k}" ${p.grant.departments.includes(k)?'checked':''}>${v}</label>`).join('')}</fieldset><p class="dp-note">无有效期；管理员手动撤销前持续有效。<br>만료일 없이 관리자가 해제할 때까지 유지됩니다.</p>`}<label>核实原因 / 확인 사유<textarea name="reason" required maxlength="500" rows="2"></textarea></label><button type="submit">${revoke?'确认撤销 / 해제 확인':'保存授权 / 권한 저장'}</button></form>`,true);
  d.querySelector('form').onsubmit=e=>{e.preventDefault();const departments=revoke?p.grant.departments:[...d.querySelectorAll('[name=department]:checked')].map(n=>n.value);if(!revoke&&!departments.length){error(Error('请明确选择用工部门 / 인력 부서를 선택하세요'));return;}save('update',{person_id:p.id,badge:p.badge,version:p.grant.version,enabled:!revoke,departments,reason:d.querySelector('[name=reason]').value.trim()});};
 }
 function link(p){
  frame('关联本人临时牌 <small>본인 임시 명찰 연결</small>',`<p>已选择的本人长期档案 / 선택한 본인 고정 기록</p>${identity(p)}<p class="dp-note">核对工牌和本人身份后关联；修改当天姓名不会获得授权。关联保留当天签到和历史作业。<br>명찰과 본인을 확인하세요. 이름 수정만으로 권한이 생기지 않으며 기존 기록은 유지됩니다.</p><form data-preview-form><label>扫描当天临时工牌 / 오늘 임시 명찰<input name="daily_badge" required autocomplete="off" placeholder="DA-…"></label><button type="submit">读取并核对 / 조회·확인</button></form><div data-preview></div>`,true);
  let verifying=false,scanVersion=0;d.querySelector('[data-preview-form]').onsubmit=async e=>{e.preventDefault();if(verifying)return;verifying=true;const scan=++scanVersion,version=epoch,badge=d.querySelector('[name=daily_badge]').value.trim(),button=e.submitter;button.disabled=true;d.querySelector('[data-preview]').innerHTML='';d.querySelector('[data-error]').hidden=true;
   try{const r=await request('link_preview',{person_id:p.id,daily_badge:badge});if(epoch!==version||scan!==scanVersion)return;d.querySelector('[data-preview]').innerHTML=`<section class="dp-preview"><h3>请核实是同一人 / 동일인 확인</h3>${identity(r.daily)}<p>签到日期 / 출근일：${esc(r.day)}</p><p>将关联至 / 연결 대상：<strong>${esc(p.name)}</strong><br>${esc(p.badge)}</p>${r.linked?'<p>已关联，无需重复保存 / 이미 연결됨</p>':'<form data-confirm-link><label class="dp-check"><input type="checkbox" required>已当面核对为同一本人 / 본인 확인 완료</label><label>核实说明 / 확인 사유<textarea name="reason" required maxlength="500" rows="2"></textarea></label><button type="submit">确认关联 / 연결 확인</button></form>'}</section>`;
   const form=d.querySelector('[data-confirm-link]');if(form)form.onsubmit=ev=>{ev.preventDefault();save('link',{person_id:p.id,badge:p.badge,daily_badge:r.daily.badge,daily_person_id:r.daily.id,attendance_id:r.attendance_id,reason:form.elements.reason.value.trim()});};
   }catch(x){if(epoch===version)error(x);}finally{verifying=false;if(button.isConnected)button.disabled=false;}
  };d.querySelector('[name=daily_badge]').oninput=()=>{scanVersion++;d.querySelector('[data-preview]').innerHTML='';};
 }
 async function history(p){frame('权限记录 <small>권한 이력</small>',`${identity(p)}<div data-history-list>正在读取 / 불러오는 중</div>`,true);const version=epoch;try{const r=await request('history',{person_id:p.id});if(epoch!==version)return;d.querySelector('[data-history-list]').innerHTML=r.items.map(x=>`<article class="dp-event"><strong>${x.action.endsWith('_link')?'关联临时牌 / 임시 명찰 연결':x.after.enabled?'开启 / 调整 / 허용·변경':'撤销 / 해제'}</strong><small>${esc(x.actor_name)} · ${esc(time(x.at))}</small><p>${x.action.endsWith('_link')?esc(p.links.find(l=>l.daily_person_id===x.after.daily_person_id)?.badge_id||x.after.daily_person_id):esc(labels(JSON.parse(x.before.departments||'[]')))+' → '+esc(labels(JSON.parse(x.after.departments||'[]')))}</p><p>${esc(x.reason)}</p></article>`).join('')||'<p class="dp-empty">暂无记录 / 이력 없음</p>';}catch(e){if(epoch===version)error(e);}}
 d.showModal();await list();d.querySelector('[data-search]')?.focus({preventScroll:true});
};
})();
