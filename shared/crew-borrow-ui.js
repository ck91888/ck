(function(){
 'use strict';
 window.CKInstallCrewBorrowUI=function(){
  const home=document.getElementById('page-home');if(!home)return;
  const key='ck_crew_jobs:'+CKSession.user.id,read=()=>{try{return JSON.parse(sessionStorage.getItem(key)||'[]');}catch{return [];}};
  const jobs=new Set(read()),panel=document.createElement('section');panel.className='card ck-crew-returns';panel.hidden=true;panel.setAttribute('role','status');home.append(panel);
  let last=[],loading=false,again=false;
  const job=(b,prefix)=>({id:b[prefix+'_job_id'],business_no:b[prefix+'_business_no'],...(Object.hasOwn(b,prefix+'_display_business_no')?{display_business_no:b[prefix+'_display_business_no'],has_business_reference:b[prefix+'_has_business_reference']}:{ }),job_title:b[prefix+'_job_title'],job_label:b[prefix+'_job_label'],dispatcher_name:b[prefix+'_dispatcher_name'],job_type:b[prefix+'_job_type'],job_department:b[prefix+'_job_department'],job_started_at:b[prefix+'_job_started_at']});
  const route=b=>'原作业 / 원작업：'+window.CKDocumentLabels.jobSummary(job(b,'source'))+' → 装卸作业 / 상하차：'+window.CKDocumentLabels.jobSummary(job(b,'destination'));
  const reason=value=>window.CKDocumentLabels.jobLeaveReason(value);

  const save=()=>sessionStorage.setItem(key,JSON.stringify([...jobs]));
  async function refresh(){
   if(loading){again=true;return;}loading=true;
   try{
    panel.replaceChildren();let shown=false;
    for(const id of [...jobs]){
     try{const r=await CKSession.request('sop_crew_status',{job_id:id}),open=r.items.filter(b=>['borrowed','return_pending'].includes(b.status));if(!open.length){jobs.delete(id);continue;}
      shown=true;const row=document.createElement('div'),text=document.createElement('p'),button=document.createElement('button');
      text.textContent=open.map(b=>b.worker_name+' · '+(b.status==='borrowed'?'装卸借调中 / 지원 중':reason(b.blocked_reason)||'待归还 / 복귀 대기')+' · '+route(b)).join('；');button.type='button';button.className='btn btn-outline';button.textContent='核对并重试归还 / 복귀 재시도';
      button.onclick=async()=>{button.disabled=true;try{const result=await CKSession.request('sop_crew_return',{job_id:id});last=result.crew_returns||[];await refresh();}catch(e){text.textContent=e.message;}finally{button.disabled=false;}};row.append(text,button);panel.append(row);
     }catch(e){shown=true;const p=document.createElement('p');p.textContent='借调记录暂未取得：'+e.message;panel.append(p);}
    }
    if(last.length){shown=true;const p=document.createElement('p');p.textContent=last.map(b=>(b.worker_name||b.worker_id)+' · '+(b.status==='returned'?'已归还原任务 / 원작업 복귀':reason(b.reason)||'归还待核对 / 복귀 확인 필요')+' · '+route(b)).join('；');panel.append(p);}
    panel.hidden=!shown;save();
   }finally{loading=false;if(again){again=false;refresh();}}
  }
  function track(r,id){
   if(r?.has_crew_borrows&&r.job_id)jobs.add(r.job_id);
   if(r?.crew_return_error&&id)jobs.add(id);
   if(r?.crew_returns?.length){last=r.crew_returns;for(const b of last)if(b.destination_job_id)jobs.add(b.destination_job_id);if(id)jobs.add(id);}
   if(r?.has_crew_borrows||r?.crew_return_error||r?.crew_returns?.length){save();refresh();}
  }
  window.addEventListener('ck-crew-returns',event=>track(event.detail,event.detail.job_id));
  const original=window.api;window.api=async body=>{const r=await original(body);track(r,body.job_id);return r;};
  const page=window.showPage;window.showPage=function(...args){const r=page(...args);if(args[0]==='home'&&jobs.size)refresh();return r;};
  if(jobs.size)refresh();
 };
})();
