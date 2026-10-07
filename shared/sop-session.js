/* Each entrance keeps its own HttpOnly session; field access is attendance-bound. */
(function(){
 'use strict';if(!window.CK_SOP_ROLLOUT?.staging)return;
 document.documentElement.classList.add('ck-auth-pending');
 const scope=location.pathname.startsWith('/001/')?'field':location.pathname.startsWith('/attendance/')?'kiosk':'office';
 const endpoint=window.SOP_API||'/api';let user=null,resolveReady,gate,scanner,locked=false,completed=false,operationId='';
 const ready=new Promise(r=>resolveReady=r);
 const rawFetch=window.fetch.bind(window);
 // Share only identical in-flight reads. No response cache, mutation retry, or cross-login data.
 const reads=new Set('sop_identity sop_get sop_list sop_need_groups sop_linked sop_updates sop_work_materials sop_batch_work_materials sop_work_need_search sop_dispatch_list v2_inbound_plan_detail v2_outbound_order_detail v2_ops_job_detail v2_attachment_list sop_attendance_summary'.split(' '));
 const inFlight=new Map();let generation=0;
 window.fetch=async function(input,init){
  const url=new URL(typeof input==='string'?input:input.url,location.href),local=url.origin===location.origin&&url.pathname===new URL(endpoint,location.href).pathname;
  let key='',body;
  if(local){const headers=new Headers(init?.headers||(typeof input==='object'?input.headers:undefined));if(scope==='office'&&operationId)headers.set('X-CK-Operation-Context',operationId);init={...init,headers};try{body=JSON.parse(init?.body);}catch{}if(reads.has(body?.action)&&!init?.signal)key=generation+':'+JSON.stringify(body);else{generation++;inFlight.clear();}}
  let pending=key&&inFlight.get(key);
  if(!pending){pending=rawFetch(input,init);if(key){inFlight.set(key,pending);pending.finally(()=>{if(inFlight.get(key)===pending)inFlight.delete(key);}).catch(()=>{});}}
  const original=await pending,r=key?original.clone():original;
  if(completed&&r.status===401&&local)lock();
  if(local&&scope==='office'&&r.status===409){const out=await r.clone().json().catch(()=>({}));if(out.operator_context_changed)operationLock();}
  return r;
 };

 async function request(action,data={}){
  const r=await fetch(endpoint,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({...data,action})});
  const out=await r.json();if(!r.ok||!out.ok){const error=Error(out.error||out.message||'请求失败');error.businessError=true;error.unauthorized=r.status===401;error.operatorContextChanged=!!out.operator_context_changed;throw error;}if(out.has_crew_borrows||out.crew_returns||out.crew_return_error)window.dispatchEvent(new CustomEvent('ck-crew-returns',{detail:{job_id:data.job_id,...out}}));return out;
 }
 async function stopCamera(){if(scanner){const old=scanner;scanner=null;try{await old.stop();old.clear();}catch{}}}
 function lock(){if(locked)return;locked=true;user=null;document.documentElement.classList.add('ck-auth-pending');document.querySelectorAll('dialog[open]').forEach(d=>d.close());showGate('登录已结束，请重新扫码 / 로그인이 종료되었습니다');}
 window.CKSession={ready,get user(){return user;},scope,request,changeOperator:()=>operatorForm(user,true),async logout(){await request('sop_logout');sessionStorage.removeItem('ck_operation_context');sessionStorage.removeItem('ck_operation_reconfirm');sessionStorage.removeItem('ck_test_active_lead');location.reload();}};
 function showGate(message=''){
  gate?.remove();gate=document.createElement('section');gate.id='ck-auth-gate';gate.className='ck-access-gate';
  const field=scope==='field',kiosk=scope==='kiosk';
  gate.innerHTML='<form><div class="ck-login-mark" aria-hidden="true">CK</div><h1>'+(field?'现场执行':kiosk?'签到点启用':'办公室登录')+'</h1><p class="ck-login-ko">'+(field?'현장 실행 로그인':kiosk?'출퇴근 등록 단말 활성화':'사무실 로그인')+'</p><p class="ck-login-hint">'+(field?'先上班签到，再扫描职员工牌。<br>출근 등록 후 직원 명찰을 스캔하세요.':kiosk?'由管理员在这台签到电脑上启用一次。管理员密码为5至128个字符。<br>관리자가 이 단말을 활성화해 주세요. 비밀번호는 5–128자입니다.':'使用管理员密码进入协同中心、耗材和看板。密码为5至128个字符；尚未配置密码时可使用原授权码。<br>관리자 비밀번호로 로그인하세요. 비밀번호는 5–128자입니다. 설정 전에는 기존 인증 코드를 사용할 수 있습니다.')+'</p><label for="ck-login-code">'+(field?'职员工牌 / 직원 명찰':'管理员密码 / 관리자 비밀번호')+'</label><input id="ck-login-code" name="code" type="'+(field?'text':'password')+'" autocomplete="off" spellcheck="false" maxlength="256" required placeholder="'+(field?'扫描工牌 / 명찰 스캔':'输入管理员密码 / 비밀번호 입력')+'"><button type="submit" class="ck-login-submit">'+(field?'进入现场系统 / 로그인':kiosk?'启用签到点 / 활성화':'登录 / 로그인')+'</button>'+(field?'<button type="button" class="ck-camera-button">相机扫工牌 / 카메라 스캔</button><div id="ck-login-reader"></div><p class="ck-login-foot">仅限已授权、当天已签到的职员或派审员。<br>승인된 출근 직원만 이용할 수 있습니다.</p>':'')+'<p role="alert" aria-live="polite"></p></form>';
  document.body.append(gate);const form=gate.querySelector('form'),input=form.elements.code,messageEl=gate.querySelector('[role=alert]');messageEl.textContent=message;
  form.onsubmit=async e=>{e.preventDefault();const button=form.querySelector('[type=submit]');if(button.disabled)return;button.disabled=true;messageEl.textContent='正在核验 / 확인 중';try{const r=await request('sop_login',field?{badge:input.value}:{sop_key:input.value});input.value='';await stopCamera();if(completed){sessionStorage.removeItem('ck_operation_context');sessionStorage.setItem('ck_operation_reconfirm','1');location.reload();return;}await accept(r);}catch(x){messageEl.textContent=x.message;input.select();}finally{button.disabled=false;}};
  const camera=gate.querySelector('.ck-camera-button');if(camera)camera.onclick=async()=>{camera.disabled=true;try{if(!window.Html5Qrcode)throw Error('相机未就绪，可使用扫码枪 / 스캐너를 사용하세요');scanner=new Html5Qrcode('ck-login-reader');await scanner.start({facingMode:'environment'},{fps:10,qrbox:{width:220,height:220}},async code=>{input.value=code;await stopCamera();form.requestSubmit();});}catch(x){messageEl.textContent='相机无法打开，请使用扫码枪 / 카메라를 열 수 없습니다';camera.disabled=false;}};
  input.focus();
 }
 async function accept(r){
  if(scope==='office'){if(!r.user.operation_context)r=await request('sop_identity');const ctx=r.user.operation_context;if(!ctx){complete(r);return;}operationId=ctx.id;const saved=sessionStorage.getItem('ck_operation_context');if(saved&&saved!==ctx.id){operationLock(r.user);return;}if(!ctx.name||sessionStorage.getItem('ck_operation_reconfirm')){operatorForm(r.user);return;}sessionStorage.setItem('ck_operation_context',ctx.id);}
  complete(r);
 }
 function operationLock(current){if(locked)return;locked=true;user=null;document.documentElement.classList.add('ck-auth-pending');document.querySelectorAll('dialog[open]').forEach(d=>d.close());gate?.remove();gate=document.createElement('section');gate.id='ck-auth-gate';gate.className='ck-access-gate';gate.innerHTML='<h1>操作人员已变化 / 작업자 변경</h1><p>本页已停止提交。请先保留未提交内容，然后重新加载并确认操作姓名。<br>저장하지 않은 내용을 보관한 뒤 새로고침하고 작업자 이름을 확인하세요.</p><p data-current></p><button type=button>重新加载并确认 / 새로고침 후 확인</button><button type=button data-view-draft>查看未提交内容（提交仍停用） / 미저장 내용 보기</button>';if(current)gate.querySelector('[data-current]').textContent='当前操作标签 / 현재 작업자: '+(current.operation_context?.name||'尚未确认');gate.querySelector('[data-view-draft]').onclick=()=>{document.documentElement.classList.remove('ck-auth-pending');gate.scrollIntoView();};gate.querySelector('button').onclick=()=>{sessionStorage.removeItem('ck_operation_context');sessionStorage.setItem('ck_operation_reconfirm','1');location.reload();};document.body.append(gate);const label=document.querySelector('[data-session-name]');if(label)label.textContent='操作姓名待重新确认 / 작업자 재확인 필요';const badge=document.getElementById('userBadge');if(badge)badge.textContent='待确认 / 확인 필요';}
 function operatorForm(u,changing=false){
  if(!u?.operation_context)return;operationId=u.operation_context.id;
  const host=document.createElement('section');host.id=changing?'ck-operator-host':'ck-auth-gate';if(!changing){gate?.remove();gate=host;}
  host.innerHTML='<dialog class="ck-workflow ck-operator-dialog" aria-labelledby="ck-operator-title"><form><h2 id="ck-operator-title">确认操作姓名 / 작업자 이름 확인</h2><p>计划和操作记录将使用此姓名。此姓名由本人填写，管理员账号权限保持原样。<br>계획과 작업 기록에 사용할 이름입니다. 직접 입력한 이름이며 관리자 계정 권한은 그대로입니다.</p>'+(changing?'<p>确认切换后将重新加载本页，未提交内容请先保留。<br>변경 후 페이지를 새로고침합니다. 저장하지 않은 내용은 먼저 보관하세요.</p>':'')+'<label for="ck-operator-name">本次登录员姓名 / 이번 작업자 이름<input id="ck-operator-name" name="operator" autocomplete="off" maxlength="80" required></label><p role="alert" aria-live="polite"></p><div class="actions"><button type="submit" data-operator-confirm>确认并继续 / 확인 후 계속</button><button type="button" class="light" data-operator-cancel>'+(changing?'取消 / 취소':'退出登录 / 로그아웃')+'</button></div></form></dialog>';document.body.append(host);const dialog=host.querySelector('dialog'),form=host.querySelector('form'),input=form.elements.operator,msg=host.querySelector('[role=alert]');if(changing||sessionStorage.getItem('ck_operation_reconfirm'))input.value=u.operation_context.name||'';
  const cancel=()=>{if(changing){dialog.close();host.remove();}else CKSession.logout().catch(e=>msg.textContent=e.message);};host.querySelector('[data-operator-cancel]').onclick=cancel;dialog.oncancel=e=>{e.preventDefault();if(changing)cancel();else msg.textContent='请填写操作姓名，或退出登录 / 이름을 입력하거나 로그아웃하세요';};
  form.onsubmit=async e=>{e.preventDefault();const submit=form.querySelector('[type=submit]');if(submit.disabled)return;submit.disabled=true;msg.textContent='正在确认 / 확인 중';try{const r=await request('sop_operator_confirm',{name:input.value});sessionStorage.setItem('ck_operation_context',r.user.operation_context.id);sessionStorage.removeItem('ck_operation_reconfirm');operationId=r.user.operation_context.id;dialog.close();host.remove();if(changing){location.reload();return;}complete(r);}catch(x){msg.textContent=x.businessError?x.message:'网络暂时不可用，请保留姓名并重试 / 네트워크 연결 후 이름을 다시 확인하세요';input.focus();}finally{submit.disabled=false;}};dialog.showModal();input.focus();
 }
 function complete(r){user=r.user;locked=false;completed=true;gate?.remove();document.documentElement.classList.remove('ck-auth-pending');const banner=document.querySelector('.ck-stage-banner');const label=document.createElement('span');label.dataset.sessionName='true';label.textContent=scope==='kiosk'?'签到点已启用 / 등록 단말 활성화':user.name;banner.append(label);const out=document.createElement('button');out.type='button';out.textContent=scope==='field'?'退出 / 切换人员 · 로그아웃':scope==='kiosk'?'停用本机 / 단말 로그아웃':'退出';out.onclick=()=>CKSession.logout().catch(e=>alert(e.message));banner.append(out);resolveReady(user);}
 document.addEventListener('DOMContentLoaded',async()=>{
  const banner=document.createElement('div');banner.className='ck-stage-banner';banner.innerHTML='<b>CK · 测试环境 / 테스트</b><span>'+(scope==='field'?'现场执行专用 / 현장 전용':scope==='kiosk'?'上下班签到点 / 출퇴근 등록':'办公室管理 / 사무실 관리')+'</span>';document.body.prepend(banner);
  showGate();try{await accept(await request('sop_identity'));}catch(e){if(!e.unauthorized)gate.querySelector('[role=alert]').textContent=e.message;}
  let checking=false;
  const check=async()=>{if(!completed||locked||document.hidden||checking)return;checking=true;try{const r=await request('sop_identity');if(scope==='office'&&r.user.operation_context?.id!==operationId)operationLock(r.user);}catch(e){if(e.unauthorized)lock();}finally{checking=false;}};
  setInterval(check,15000);window.addEventListener('focus',check);document.addEventListener('visibilitychange',check);
  if(scope==='field'){
   const clean=()=>document.querySelectorAll('a[href]').forEach(a=>{const u=new URL(a.href,location.href);if(u.origin!==location.origin||!u.pathname.startsWith('/001/')){const span=document.createElement('span');span.textContent=a.textContent;span.className=a.className;a.replaceWith(span);}});
   new MutationObserver(clean).observe(document.body,{childList:true,subtree:true});clean();
   document.addEventListener('click',e=>{const a=e.target.closest('a');if(a){const u=new URL(a.href,location.href);if(u.origin!==location.origin||!u.pathname.startsWith('/001/')){e.preventDefault();e.stopImmediatePropagation();}}},true);
  }
 });
})();
