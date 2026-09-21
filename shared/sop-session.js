/* Isolated acceptance login, shared by the original four applications. */
(function(){
 'use strict';if(!window.CK_SOP_ROLLOUT?.staging)return;
 document.documentElement.classList.add('ck-auth-pending');
 let user=null,resolveReady;const ready=new Promise(r=>resolveReady=r);
 async function request(action,data={}){
  const r=await fetch('/api',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...data})});
  const out=await r.json();if(!r.ok||!out.ok){const error=Error(out.error||out.message||'请求失败');error.businessError=true;throw error;}return out;
 }
 window.CKSession={ready,get user(){return user;},request,async logout(){await request('sop_logout');location.reload();}};
 document.addEventListener('DOMContentLoaded',async()=>{
  const banner=document.createElement('div');banner.className='ck-stage-banner';banner.innerHTML='<b>原系统升级 · 独立测试环境</b><span>仅使用虚拟数据 / 테스트 전용</span><a href="/">系统首页</a><a href="/验收说明.html">验收说明</a>';
  document.body.prepend(banner);
  const gate=document.createElement('section');gate.id='ck-auth-gate';gate.innerHTML='<form><h2>CK 仓库系统 · 测试登录</h2><p>登录一次即可在现场执行、协同中心、耗材和看板之间切换。</p><label>个人授权码<input name="key" type="password" autocomplete="current-password" required></label><button>进入测试系统</button><p role="alert"></p></form>';document.body.append(gate);
  function complete(r){user=r.user;gate.remove();document.documentElement.classList.remove('ck-auth-pending');banner.insertAdjacentHTML('beforeend','<button type="button" id="ck-session-logout">退出</button>');document.getElementById('ck-session-logout').onclick=()=>window.CKSession.logout();resolveReady(user);}
  gate.querySelector('form').onsubmit=async e=>{e.preventDefault();const button=gate.querySelector('button');button.disabled=true;gate.querySelector('[role="alert"]').textContent='登录中…';try{const r=await request('sop_login',{sop_key:gate.querySelector('input').value});gate.querySelector('input').value='';complete(r);}catch(e){gate.querySelector('[role="alert"]').textContent=e.message;}finally{button.disabled=false;}};
  try{complete(await request('sop_identity'));}catch{gate.querySelector('[role="alert"]').textContent='请输入此前设置的个人授权码。';}
 });
})();
