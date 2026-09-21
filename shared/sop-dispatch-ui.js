(function(){
 'use strict';
 const startActions=new Set(['v2_unload_job_start','v2_unplanned_unload_start','v2_inbound_job_start','v2_import_delivery_job_start','v2_outbound_load_start','v2_outbound_stock_op_start','v2_issue_handle_start','v2_pick_job_start','v2_pick_job_start_by_docs','v2_bulk_op_job_start','v2_ops_job_start','v2_verify_job_start']);
 window.CKInstallDispatch=function(){
  let lead=null;try{lead=JSON.parse(sessionStorage.getItem('ck_test_active_lead')||'null');}catch{}
  window.getWorkerId=()=>lead?.id||'';window.getWorkerName=()=>lead?.name||'';window.getBadge=()=>lead?lead.id+'|'+lead.name:'';
  window.CKOpenNativeJob=function(job){lead=job.workers.find(w=>w.id===job.lead_id);sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));saveActiveJob(job.id,null);if(job.job_type==='issue_handle')window._currentIssueId=job.source_id;goMyTask();};
  window.CKClearNativeJob=function(){lead=null;sessionStorage.removeItem('ck_test_active_lead');clearActiveJob();};
  // The manager dispatches several crews; the backend still checks every worker's occupancy.
  window.hasOtherActiveJob=()=>false;
  window.checkMyActiveJob=async()=>{};window.restoreActiveJob=()=>{};
  const original=window.api;const retries=new Map();
  window.api=async function(body){
   if(!startActions.has(body.action))return original(body);
   const fingerprint=JSON.stringify(Object.fromEntries(Object.entries(body).filter(([k])=>!['client_req_id','worker_id','worker_name','handler_id','handler_name'].includes(k)).sort(([a],[b])=>a.localeCompare(b))));
   let pending=retries.get(fingerprint);
   if(!pending){const staff=await chooseStaff();if(!staff)return {ok:false,error:'已取消派工'};pending={payload:{...body,client_req_id:body.client_req_id||crypto.randomUUID()},staff};retries.set(fingerprint,pending);}
   const staff=pending.staff;
   try{
    const r=await CKSession.request('sop_native_start',{payload:pending.payload,...staff});
    retries.delete(fingerprint);lead=r.lead||staff.workers.find(x=>x.id===staff.lead_id);sessionStorage.setItem('ck_test_active_lead',JSON.stringify(lead));
    return r;
   }catch(e){if(e.businessError&&!e.message.includes('任务已建立'))retries.delete(fingerprint);return {ok:false,error:e.message};}
  };
 };
 function chooseStaff(){return new Promise(resolve=>{
  let scanner=null,people=[];const dialog=document.createElement('dialog');dialog.className='ck-workflow';dialog.style.cssText='width:min(620px,94vw);max-height:90vh;overflow:auto;padding:20px;border:1px solid #ccd6e0;border-radius:8px';
  dialog.innerHTML='<form><h2>负责人分配本次作业人员</h2><p>扫描实际操作人员工牌；确认后按原业务流程开始计时。</p><label>工牌二维码<input name="badge" placeholder="TEST-A|测试操作员甲"></label><button type="button" data-add>添加工牌</button> <button type="button" data-camera>相机扫码</button><div id="ck-native-scanner"></div><ul data-people></ul><label>主操作员<select name="lead" required></select></label><label>预计操作分钟<input name="minutes" type="number" min="1" step="1" value="30" required></label><p role="alert"></p><button type="submit">确认人员并开始</button> <button type="button" data-cancel>取消</button></form>';
  document.body.append(dialog);const form=dialog.querySelector('form');
  const render=()=>{dialog.querySelector('[data-people]').replaceChildren();form.elements.lead.replaceChildren();for(const p of people){const li=document.createElement('li');li.textContent=p.name+' ('+p.id+') ';const remove=document.createElement('button');remove.type='button';remove.textContent='移除';remove.onclick=()=>{people=people.filter(x=>x.id!==p.id);render();};li.append(remove);dialog.querySelector('[data-people]').append(li);const option=new Option(p.name,p.id);form.elements.lead.add(option);}};
  const add=value=>{const p=value.trim().split('|');if(p.length!==2||!p[0]||!p[1])throw Error('请扫描“工号|姓名”格式的工牌');if(!people.some(x=>x.id===p[0]))people.push({id:p[0],name:p[1]});form.elements.badge.value='';render();};
  dialog.querySelector('[data-add]').onclick=()=>{try{add(form.elements.badge.value);}catch(e){dialog.querySelector('[role=alert]').textContent=e.message;}};
  form.elements.badge.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();dialog.querySelector('[data-add]').click();}};
  dialog.querySelector('[data-camera]').onclick=async()=>{try{if(scanner){await scanner.stop();scanner=null;return;}scanner=new Html5Qrcode('ck-native-scanner');await scanner.start({facingMode:'environment'},{fps:8,qrbox:200},value=>{try{add(value);}catch(e){dialog.querySelector('[role=alert]').textContent=e.message;}},()=>{});}catch{dialog.querySelector('[role=alert]').textContent='无法打开相机，可使用扫码枪或输入工牌内容';scanner=null;}};
  const done=async value=>{if(scanner)await scanner.stop().catch(()=>{});dialog.close();dialog.remove();resolve(value);};
  dialog.querySelector('[data-cancel]').onclick=()=>done(null);dialog.oncancel=e=>{e.preventDefault();done(null);};
  form.onsubmit=e=>{e.preventDefault();if(!people.length){dialog.querySelector('[role=alert]').textContent='至少登记一名操作人员';return;}done({workers:people,lead_id:form.elements.lead.value,estimated_minutes:Number(form.elements.minutes.value)});};dialog.showModal();form.elements.badge.focus();
 });}
})();
