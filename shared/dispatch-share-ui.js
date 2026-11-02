(function(){
'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const request=(action,data)=>CKSession.request(action,data);
function person(p){return esc((p?.name||'未命名')+(p?.badge?' · '+p.badge:''));}
window.CKDispatchShare={
 async install(home){
  if(!home||home.querySelector('[data-dispatch-share]'))return;
  let status;try{status=await request('sop_dispatch_share_status');}catch{return;}
  if(!status?.enabled)return;
  const open=document.createElement('button');open.type='button';open.className='btn btn-outline';open.dataset.dispatchShare='';open.textContent='派审协作 / 배정 협업';
  open.onclick=()=>panel();home.insertBefore(open,home.firstChild);
 },
 async panel(){return panel();}
};
async function panel(){
 const dialog=document.createElement('dialog');dialog.className='ck-workflow';
 dialog.innerHTML='<form><h2>派审协作 / 배정 협업</h2><p>扫描另一位大货派审员的工牌。对方确认后，双方未完成的大货单和之后新派的单可以互相处理。原派审员不变。退出后对方不能再操作。</p><div data-share-body>正在读取…</div><label>对方工牌 / 상대 명찰<input name="badge" autocomplete="off" placeholder="工号|姓名"></label><p role="alert"></p><div class="ck-buttons"><button type="submit">发出邀请 / 초대</button><button type="button" data-close>关闭 / 닫기</button></div></form>';
 document.body.append(dialog);dialog.showModal();
 const form=dialog.querySelector('form'),body=dialog.querySelector('[data-share-body]'),error=dialog.querySelector('[role=alert]'),badge=form.elements.badge;
 const camera=window.CKAttendance?.badgeCamera?.(form,badge,{parseBadge:raw=>raw,recognizedMessage:'已识别工牌，请确认后发出邀请'});
 let busy=false,requestId='';
 const close=async()=>{if(busy)return;await camera?.destroy();dialog.close();dialog.remove();window.loadDispatchTasks?.();};
 dialog.querySelector('[data-close]').onclick=close;dialog.oncancel=e=>{e.preventDefault();void close();};
 async function paint(){
  const status=await request('sop_dispatch_share_status');
  body.innerHTML=[
   section('等待我确认',status.incoming,p=>'<button type="button" data-accept="'+esc(p.id)+'">确认与 '+person(p.inviter)+' 协作</button>'),
   section('等待对方确认',status.outgoing,p=>'<button type="button" data-cancel="'+esc(p.id)+'">取消邀请 '+person(p.invitee)+'</button>'),
   section('正在协作',status.partners,p=>'<button type="button" data-leave="'+esc(p.id)+'">退出与 '+person(p.partner)+' 的协作</button>')
  ].join('')||'<p>当前没有协作。邀请确认后才会共享未完成的大货单。</p>';
  body.querySelectorAll('button').forEach(button=>button.onclick=async()=>{
   const action=button.dataset.accept?'sop_dispatch_share_accept':button.dataset.cancel?'sop_dispatch_share_cancel':'sop_dispatch_share_leave';
   const id=button.dataset.accept||button.dataset.cancel||button.dataset.leave;
   button.disabled=true;try{await request(action,{pair_id:id,client_req_id:crypto.randomUUID()});await paint();}catch(e){error.textContent=e.message;button.disabled=false;}
  });
 }
 function section(title,items,button){if(!items?.length)return '';return '<section><h3>'+esc(title)+'</h3>'+items.map(button).join('')+'</section>';}
 form.onsubmit=async e=>{e.preventDefault();if(busy)return;const value=badge.value.trim();if(!value)return;busy=true;form.querySelector('[type=submit]').disabled=true;
  try{if(!requestId)requestId=crypto.randomUUID();await request('sop_dispatch_share_invite',{target_badge:value,client_req_id:requestId});requestId='';badge.value='';error.textContent='邀请已发出，请对方在自己的手机上确认';await paint();}
  catch(err){requestId='';error.textContent=err.message;}finally{busy=false;form.querySelector('[type=submit]').disabled=false;}
 };
 try{await paint();}catch(e){error.textContent=e.message;}
}
})();
