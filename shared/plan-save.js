(function(){'use strict';
 const vault=new Map(),prefix='ck-plan-receipt:',limits={uploadMs:60000,submitMs:30000,queryMs:8000};
 const owner=()=>{const u=window.CKSession?.user;return u?JSON.stringify([u.scope||'',u.id,u.operation_context?.id||'personal-session']):'';};
 function assert(p){if(!owner()||owner()!==p.owner){const x=Error('登录身份已变化，此草稿不会跨账号提交。请用原身份核查原保存结果。 / 로그인 정보가 변경되었습니다.');x.identityChanged=true;throw x;}}
 const key=(o,s)=>prefix+encodeURIComponent(o)+'/'+encodeURIComponent(JSON.stringify(s));
 function persist(p){if(!p.complete&&p.sent)try{sessionStorage.setItem(p.key,JSON.stringify({request_id:p.request_id,sent:true}));}catch{}}
 function emit(p,text=p.text){p.text=text;if(p.listener&&owner()===p.owner)p.listener(p);}
 function stage(p,phase,text){p.phase=phase;p.stepAt=Date.now();emit(p,text);}
 function headers(){const h={'Content-Type':'application/json'},id=CKSession.user?.operation_context?.id;if(id)h['X-CK-Operation-Context']=id;return h;}
 async function post(p,action,data,ms){
  assert(p);const controller=new AbortController();p.controller=controller;const timer=setTimeout(()=>controller.abort('timeout'),ms);
  try{const response=await fetch(window.SOP_API||'/api',{method:'POST',credentials:'same-origin',headers:headers(),body:JSON.stringify({...data,action}),signal:controller.signal});let out;try{out=await response.json();}catch{throw Error('服务器响应未完整收到');}assert(p);if(!response.ok||!out.ok){const x=Error(out.error||'保存请求未成功');x.businessError=response.status<500&&!!out.error;x.status=response.status;throw x;}return out;
  }catch(x){if(x.identityChanged||x.businessError)throw x;const error=Error(controller.signal.aborted?'请求等待超时或已关闭，后台状态仍需查询。':'网络响应未完整收到，后台状态仍需查询。');error.transportError=true;throw error;}
  finally{clearTimeout(timer);if(p.controller===controller)p.controller=null;}
 }
 function open(source){
  const who=owner();if(!who)throw Error('请先登录');for(const [k,p]of vault)if(p.owner!==who){p.controller?.abort();p.listener=null;vault.delete(k);}
  const id=key(who,source);let p=vault.get(id);if(p)return p;
  let receipt=null;try{receipt=JSON.parse(sessionStorage.getItem(id)||'null');}catch{}
  p={key:id,owner:who,source,request_id:receipt?.request_id||crypto.randomUUID(),sent:!!receipt?.sent,unknown:!!receipt?.sent,receiptOnly:!!receipt?.sent,body:null,busy:false,phase:receipt?.sent?'unknown':'idle',text:'',form:null,cargo:null,outbounds:null,controller:null,result:null,complete:false,listener:null};vault.set(id,p);return p;
 }
 async function check(p){assert(p);stage(p,'checking','正在查询本次提交是否已经保存… / 저장 결과 확인 중');const r=await post(p,'sop_need_create_status',{request_id:p.request_id,...p.source},limits.queryMs);if(r.state==='committed'){p.result=r.result;p.unknown=false;stage(p,'committed','已确认保存成功 / 저장 완료 확인');return r.result;}p.unknown=!!p.sent;stage(p,p.sent?'unknown':'idle',p.sent?'暂未确认保存结果；查不到不等于未保存。原输入、附件和编号已保留。 / 저장 여부 미확인':'尚未发送作业提交 / 작업 제출 전');return null;}
 async function send(p,body){assert(p);const alreadyUncertain=p.sent&&p.unknown;p.body=body;p.sent=true;p.unknown=true;p.receiptOnly=false;persist(p);stage(p,'sending','正在保存作业计划，请勿重复新建… / 작업 계획 저장 중');try{const r=await post(p,'sop_need_create',body,limits.submitMs);p.result=r;p.unknown=false;stage(p,'committed','已保存 / 저장 완료');return r;}catch(x){if(x.businessError&&!alreadyUncertain){p.sent=false;p.unknown=false;p.body=null;try{sessionStorage.removeItem(p.key);}catch{}stage(p,'rejected',x.message);}else{p.unknown=true;persist(p);stage(p,'unknown','保存状态未知，可能已在后台保存。请查询结果或继续原提交，不会更换编号。 / 저장 상태 미확인');}throw x;}}
 function finish(p){p.complete=true;p.listener=null;vault.delete(p.key);try{sessionStorage.removeItem(p.key);}catch{}}
 function clear(){for(const p of vault.values()){p.controller?.abort();p.listener=null;}vault.clear();try{for(let i=sessionStorage.length-1;i>=0;i--){const k=sessionStorage.key(i);if(k.startsWith(prefix))sessionStorage.removeItem(k);}}catch{}}
 async function upload(p,body,{signal,progress}={}){
  assert(p);return new Promise((resolve,reject)=>{const xhr=new XMLHttpRequest();xhr.open('POST',window.SOP_API||'/api');xhr.withCredentials=true;xhr.timeout=limits.uploadMs;const context=CKSession.user?.operation_context?.id;if(context)xhr.setRequestHeader('X-CK-Operation-Context',context);
   let settled=false;const stop=()=>xhr.abort(),end=(error,value)=>{if(settled)return;settled=true;signal?.removeEventListener('abort',stop);error?reject(error):resolve(value);};
   xhr.upload.onprogress=ev=>{try{assert(p);progress?.(ev.loaded,ev.lengthComputable?ev.total:0);}catch(x){xhr.abort();end(x);}};
   xhr.onload=()=>{try{assert(p);const out=JSON.parse(xhr.responseText);if(xhr.status<200||xhr.status>=300||!out.ok){const x=Error(out.error||'附件上传未完成');x.businessError=xhr.status<500;end(x);}else end(null,out);}catch(x){end(x);}};
   xhr.onerror=()=>end(Error('附件网络连接中断，原文件和编号已保留，请继续上传。 / 업로드 연결 끊김'));
   xhr.ontimeout=()=>end(Error('附件上传超时，文件可能已到达服务器；继续上传会复用原编号。 / 업로드 시간 초과'));
   xhr.onabort=()=>end(Error('上传已停止，草稿和原文件已保留。 / 업로드 중지·초안 유지'));
   if(signal?.aborted){end(Error('上传已停止，草稿已保留'));return;}signal?.addEventListener('abort',stop,{once:true});xhr.send(body);
  });
 }
 window.CKPlanSave={has(source){const k=key(owner(),source);try{return vault.has(k)||!!sessionStorage.getItem(k);}catch{return vault.has(k);}},limits,owner,assert,open,check,send,finish,clear,stage,emit,upload,retain(p){p.controller?.abort();p.listener=null;persist(p);},discard(p){assert(p);if(p.sent&&!p.result)throw Error('原提交状态未确认，不能放弃后重新建单');finish(p);}};
})();
