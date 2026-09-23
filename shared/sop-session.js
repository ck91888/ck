/* Each entrance keeps its own HttpOnly session; field access is attendance-bound. */
(function(){
 'use strict';if(!window.CK_SOP_ROLLOUT?.staging)return;
 document.documentElement.classList.add('ck-auth-pending');
 const scope=location.pathname.startsWith('/001/')?'field':location.pathname.startsWith('/attendance/')?'kiosk':'office';
 const endpoint=window.SOP_API||'/api';let user=null,resolveReady,gate,scanner,locked=false,completed=false;
 const ready=new Promise(r=>resolveReady=r);
 const rawFetch=window.fetch.bind(window);
 window.fetch=async function(input,init){
  const r=await rawFetch(input,init);const url=new URL(typeof input==='string'?input:input.url,location.href);
  if(completed&&r.status===401&&url.origin===location.origin&&url.pathname===new URL(endpoint,location.href).pathname)lock();
  return r;
 };
 async function request(action,data={}){
  const r=await fetch(endpoint,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({...data,action})});
  const out=await r.json();if(!r.ok||!out.ok){const error=Error(out.error||out.message||'请求失败');error.businessError=true;error.unauthorized=r.status===401;throw error;}return out;
 }
 async function stopCamera(){if(scanner){const old=scanner;scanner=null;try{await old.stop();old.clear();}catch{}}}
 function lock(){if(locked)return;locked=true;user=null;document.documentElement.classList.add('ck-auth-pending');document.querySelectorAll('dialog[open]').forEach(d=>d.close());showGate('登录已结束，请重新扫码 / 로그인이 종료되었습니다');}
 window.CKSession={ready,get user(){return user;},scope,request,async logout(){await request('sop_logout');sessionStorage.removeItem('ck_test_active_lead');location.reload();}};
 function showGate(message=''){
  gate?.remove();gate=document.createElement('section');gate.id='ck-auth-gate';gate.className='ck-access-gate';
  const field=scope==='field',kiosk=scope==='kiosk';
  gate.innerHTML='<form><div class="ck-login-mark" aria-hidden="true">CK</div><h1>'+(field?'现场执行':kiosk?'签到点启用':'办公室登录')+'</h1><p class="ck-login-ko">'+(field?'현장 실행 로그인':kiosk?'출퇴근 등록 단말 활성화':'사무실 로그인')+'</p><p class="ck-login-hint">'+(field?'先上班签到，再扫描职员工牌。<br>출근 등록 후 직원 명찰을 스캔하세요.':kiosk?'由管理员在这台签到电脑上启用一次。<br>관리자가 이 단말을 활성화해 주세요.':'使用管理员授权码进入协同中心、耗材和看板。<br>관리자 인증 코드로 로그인하세요.')+'</p><label for="ck-login-code">'+(field?'职员工牌 / 직원 명찰':'管理员授权码 / 관리자 인증 코드')+'</label><input id="ck-login-code" name="code" type="'+(field?'text':'password')+'" autocomplete="off" spellcheck="false" maxlength="256" required placeholder="'+(field?'扫描工牌 / 명찰 스캔':'输入授权码 / 인증 코드 입력')+'"><button type="submit" class="ck-login-submit">'+(field?'进入现场系统 / 로그인':kiosk?'启用签到点 / 활성화':'登录 / 로그인')+'</button>'+(field?'<button type="button" class="ck-camera-button">相机扫工牌 / 카메라 스캔</button><div id="ck-login-reader"></div><p class="ck-login-foot">仅限已授权、当天已签到的职员或派审员。<br>승인된 출근 직원만 이용할 수 있습니다.</p>':'')+'<p role="alert" aria-live="polite"></p></form>';
  document.body.append(gate);const form=gate.querySelector('form'),input=form.elements.code,messageEl=gate.querySelector('[role=alert]');messageEl.textContent=message;
  form.onsubmit=async e=>{e.preventDefault();const button=form.querySelector('[type=submit]');if(button.disabled)return;button.disabled=true;messageEl.textContent='正在核验 / 확인 중';try{const r=await request('sop_login',field?{badge:input.value}:{sop_key:input.value});input.value='';await stopCamera();if(completed){location.reload();return;}complete(r);}catch(x){messageEl.textContent=x.message;input.select();}finally{button.disabled=false;}};
  const camera=gate.querySelector('.ck-camera-button');if(camera)camera.onclick=async()=>{camera.disabled=true;try{if(!window.Html5Qrcode)throw Error('相机未就绪，可使用扫码枪 / 스캐너를 사용하세요');scanner=new Html5Qrcode('ck-login-reader');await scanner.start({facingMode:'environment'},{fps:10,qrbox:{width:220,height:220}},async code=>{input.value=code;await stopCamera();form.requestSubmit();});}catch(x){messageEl.textContent='相机无法打开，请使用扫码枪 / 카메라를 열 수 없습니다';camera.disabled=false;}};
  input.focus();
 }
 function complete(r){user=r.user;locked=false;completed=true;gate?.remove();document.documentElement.classList.remove('ck-auth-pending');const banner=document.querySelector('.ck-stage-banner');const label=document.createElement('span');label.textContent=scope==='kiosk'?'签到点已启用 / 등록 단말 활성화':user.name;banner.append(label);const out=document.createElement('button');out.type='button';out.textContent=scope==='field'?'退出 / 切换人员 · 로그아웃':scope==='kiosk'?'停用本机 / 단말 로그아웃':'退出';out.onclick=()=>CKSession.logout().catch(e=>alert(e.message));banner.append(out);resolveReady(user);}
 document.addEventListener('DOMContentLoaded',async()=>{
  const banner=document.createElement('div');banner.className='ck-stage-banner';banner.innerHTML='<b>CK · 测试环境 / 테스트</b><span>'+(scope==='field'?'现场执行专用 / 현장 전용':scope==='kiosk'?'上下班签到点 / 출퇴근 등록':'办公室管理 / 사무실 관리')+'</span>';document.body.prepend(banner);
  showGate();try{complete(await request('sop_identity'));}catch(e){if(!e.unauthorized)gate.querySelector('[role=alert]').textContent=e.message;}
  const check=async()=>{if(!completed||locked||document.hidden)return;try{await request('sop_identity');}catch(e){if(e.unauthorized)lock();}};
  setInterval(check,15000);window.addEventListener('focus',check);document.addEventListener('visibilitychange',check);
  if(scope==='field'){
   const clean=()=>document.querySelectorAll('a[href]').forEach(a=>{const u=new URL(a.href,location.href);if(u.origin!==location.origin||!u.pathname.startsWith('/001/')){const span=document.createElement('span');span.textContent=a.textContent;span.className=a.className;a.replaceWith(span);}});
   new MutationObserver(clean).observe(document.body,{childList:true,subtree:true});clean();
   document.addEventListener('click',e=>{const a=e.target.closest('a');if(a){const u=new URL(a.href,location.href);if(u.origin!==location.origin||!u.pathname.startsWith('/001/')){e.preventDefault();e.stopImmediatePropagation();}}},true);
  }
 });
})();
