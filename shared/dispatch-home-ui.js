/* Refresh only the field homepage task cards; never reinitialize a working form. */
(function(){'use strict';
window.CKDispatchHome=function(root,{page,read,render}){
 root.__ckDispatchHome?.destroy();
 const list=root.querySelector('[data-dispatch-tasks]'),status=root.querySelector('[data-dispatch-freshness]'),button=root.querySelector('[data-dispatch-refresh]');
 let disposed=false,suspended=false,active=false,generation=0,pending=null,due=false,timer=null,lastRead=0,failure='',focusRefresh=false;
 const identityKey=()=>{const u=window.CKSession?.user;return u?JSON.stringify([u.scope||'',u.id,u.attendance_id||'',u.badge||'']):'';};
 let identity=identityKey();
 function syncIdentity(){const next=identityKey();if(next===identity)return;identity=next;generation++;lastRead=0;failure='';due=true;list.replaceChildren();delete list.dataset.snapshot;}
 const alive=()=>!disposed&&root.isConnected&&root.__ckDispatchHome===controller;
 const visible=()=>alive()&&!suspended&&!document.hidden&&!document.documentElement.classList.contains('ck-auth-pending')&&page.classList.contains('active')&&!page.hidden&&page.style.display!=='none';
 const paused=()=>!!document.querySelector('dialog[open]');
 const usable=()=>active&&visible()&&!paused();
 function paint(){
  if(!alive())return;
  const old=!!lastRead&&Date.now()-lastRead>=30000;
  let message=lastRead?'任务读取于 '+new Date(lastRead).toLocaleString('zh-CN',{timeZone:'Asia/Seoul',hour12:false})+' KST / 작업 목록 확인':'尚未成功读取在途任务 / 진행 작업을 아직 불러오지 못했습니다';
  if(failure)message+=' · 刷新失败，'+(lastRead?'保留上次数据，人员状态可能已过时':'请重试')+' / 갱신 실패 · '+(lastRead?'이전 작업자 상태일 수 있습니다':'다시 시도하세요')+'：'+failure;
  else if(old)message+=' · 人员状态可能已过时 / 작업자 상태가 오래되었을 수 있습니다';
  if(active&&paused())message+=' · 操作中，自动更新已暂停 / 작업 중 자동 갱신 일시 중지';
  if(status.textContent!==message)status.textContent=message;
  status.dataset.freshness=failure?'failed':old?'stale':lastRead?'current':'loading';
  status.className=failure||old?'ck-warning':'ck-muted';button.disabled=!!pending||!usable();
  root.setAttribute('aria-busy',String(!!pending));
 }
 function draw(items){
  // Do not replace identical cards or move keyboard focus on every interval.
  const signature=JSON.stringify(items);if(signature===list.dataset.snapshot)return;
  const focused=document.activeElement,jobId=list.contains(focused)?focused.dataset.dispatchJob:'',scrollX=window.scrollX,scrollY=window.scrollY;
  const content=document.createDocumentFragment();
  for(const job of items){const card=render(job);card.dataset.dispatchJob=job.id;content.append(card);}
  if(!items.length)content.append('暂无现场在途任务 / 진행 중인 현장 작업 없음');
  list.replaceChildren(content);list.dataset.snapshot=signature;
  if(jobId){const replacement=[...list.querySelectorAll('[data-dispatch-job]')].find(card=>card.dataset.dispatchJob===jobId);if(replacement)replacement.focus({preventScroll:true});else focusRefresh=true;}
  if(window.scrollX!==scrollX||window.scrollY!==scrollY)window.scrollTo(scrollX,scrollY);
 }
 async function refresh(){
  if(!alive())return false;
  syncIdentity();
  if(!usable()){due=true;paint();return false;}
  if(pending)return pending;
  const token=generation;due=false;
  const operation=(async()=>{
   try{
    const result=await read();syncIdentity();
    if(token!==generation||!usable()){due=true;return false;}
    if(!result||!Array.isArray(result.items))throw Error('任务列表读取失败 / 작업 목록을 확인하세요');
    if(Object.hasOwn(result,'viewer_id')&&result.viewer_id!==window.CKSession?.user?.id){draw([]);lastRead=0;throw Error('登录人员已变化，请重新扫码确认身份 / 로그인 사용자가 변경되었습니다.');}
    draw(result.items);lastRead=Date.now();failure='';return true;
   }catch(error){if(token===generation&&usable())failure=error.message||'网络连接失败 / 네트워크 오류';return false;}
  })();
  pending=operation;paint();
  try{return await operation;}
  finally{if(pending===operation)pending=null;paint();if(focusRefresh&&usable()&&document.activeElement===document.body)button.focus({preventScroll:true});focusRefresh=false;if(due&&usable()){due=false;void refresh();}}
 }
 function leave(){active=false;generation++;due=true;paint();}
 function enter(){if(!alive())return Promise.resolve(false);active=true;generation++;due=true;return refresh();}
 function check(force=false){
  if(!alive()){controller.destroy();return;}
  syncIdentity();
  if(!visible()){if(active)leave();return;}
  if(!active){void enter();return;}
  paint();if(paused()){due=true;return;}if(!pending&&(force||due)){due=false;void refresh();}
 }
 const returned=()=>check(true),hidden=()=>{if(document.hidden)leave();else check(true);},sessionChanged=()=>{syncIdentity();paint();};
 const pagehide=()=>{suspended=true;leave();clearInterval(timer);timer=null;};
 const pageshow=()=>{suspended=false;if(!timer)timer=setInterval(()=>check(true),15000);check(true);};
 const observer=new MutationObserver(()=>check());
 const controller={enter,leave,refresh,destroy(){if(disposed)return;disposed=true;generation++;clearInterval(timer);observer.disconnect();window.removeEventListener('focus',returned);document.removeEventListener('visibilitychange',hidden);window.removeEventListener('pagehide',pagehide);window.removeEventListener('pageshow',pageshow);window.removeEventListener('ck-field-session-change',sessionChanged);button.onclick=null;}};
 root.__ckDispatchHome=controller;button.onclick=()=>refresh();
 observer.observe(page,{attributes:true,attributeFilter:['class','hidden','style']});
 observer.observe(document.body,{subtree:true,childList:true,attributes:true,attributeFilter:['open']});
 window.addEventListener('focus',returned);document.addEventListener('visibilitychange',hidden);window.addEventListener('pagehide',pagehide);window.addEventListener('pageshow',pageshow);window.addEventListener('ck-field-session-change',sessionChanged);
 timer=setInterval(()=>check(true),15000);paint();return controller;
};
})();
