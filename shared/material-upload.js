// Only idempotent attachment requests are retried; never plan/order creation.
(function(){'use strict';
 const limits={idleMs:90000,totalMs:900000,attempts:3,retryMs:[500,1500]},queue=[];let active=0;
 const owner=()=>{const u=window.CKSession?.user;return u?JSON.stringify([u.scope||'',u.id,u.role,u.operation_context?.id||'']):'';};
 const failure=(message,properties={})=>Object.assign(Error(message),properties);
 function validate(file){if(!file||!file.size||file.size>20*1024*1024||! /\.(pdf|xlsx?|csv|jpe?g|png|webp)$/i.test(file.name||''))throw failure((file?.name?file.name+'：':'')+'请选择20MB以内的PDF、Excel、CSV或图片 / 20MB 이하 자료 파일을 선택하세요',{businessError:true});}
 function identity(){const who=owner();return ()=>{if(!who||owner()!==who)throw failure('登录身份已变化，原文件与编号保留，请用原身份核查 / 로그인 정보가 변경되었습니다',{identityChanged:true});};}
 function pump(){while(active<2&&queue.length){const job=queue.shift();active++;Promise.resolve().then(job.run).then(job.resolve,job.reject).finally(()=>{active--;pump();});}}
 const slot=run=>new Promise((resolve,reject)=>{queue.push({run,resolve,reject});pump();});
 async function send(body,{signal,progress,guard=()=>{},isActive=()=>true}={}){
  const file=body.get('file');validate(file);const who=owner(),request=String(body.get('client_req_id')||''),type=String(body.get('related_doc_type')||'');
  if(body.get('action')!=='v2_attachment_upload'||!request||!body.get('related_doc_id')||!['cargo_draft','sop_need','inbound_plan'].includes(type)||type==='inbound_plan'&&body.get('attachment_category')!=='batch_work_material'||type==='sop_need'&&['inbound_material','pallet_details'].includes(body.get('attachment_category')))throw failure('资料上传编号或目标缺失，请保留原文件重新核查',{businessError:true});
  const captured=new FormData();for(const [k,v]of body)captured.append(k,v);
  const check=()=>{guard();if(!who||owner()!==who)throw failure('登录身份已变化，原文件与编号保留，请用原身份核查 / 로그인 정보가 변경되었습니다',{identityChanged:true});if(signal?.aborted||!isActive())throw failure('上传已停止，原文件和编号保留 / 업로드 중지·원본 유지',{cancelled:true});};
  const endpoint=new URL(window.SOP_API||'/api',location.href);if(endpoint.origin!==location.origin)throw failure('请从本系统页面上传资料 / 시스템 페이지에서 자료를 업로드하세요',{businessError:true});
  const attempt=number=>slot(()=>new Promise((resolve,reject)=>{
   let xhr,finished=false,idle,timedOut=false;
   const stop=()=>xhr?.abort(),end=(error,value)=>{if(finished)return;finished=true;clearTimeout(idle);signal?.removeEventListener('abort',stop);error?reject(error):resolve(value);},touch=()=>{clearTimeout(idle);idle=setTimeout(()=>{timedOut=true;xhr.abort();},limits.idleMs);};
   try{check();xhr=new XMLHttpRequest();xhr.open('POST',endpoint.href);xhr.withCredentials=true;xhr.timeout=limits.totalMs;const ctx=window.CKSession.user?.operation_context?.id;if(ctx)xhr.setRequestHeader('X-CK-Operation-Context',ctx);
    xhr.upload.onprogress=event=>{try{check();touch();progress?.(event.loaded,event.lengthComputable?event.total:file.size,{attempt:number,phase:'uploading'});}catch(error){end(error);xhr.abort();}};
    xhr.onprogress=()=>{try{check();touch();}catch(error){end(error);xhr.abort();}};
    xhr.onload=()=>{try{check();let result;try{result=JSON.parse(xhr.responseText);}catch{throw failure('服务器确认未完整收到，原文件和编号保留',{retryable:xhr.status>=200&&xhr.status<300||[408,429,500,502,503,504].includes(xhr.status),status:xhr.status});}if(xhr.status<200||xhr.status>=300||!result.ok)throw failure(result.error||'资料上传未完成',{status:xhr.status,retryable:[408,429,500,502,503,504].includes(xhr.status),businessError:xhr.status>=400&&xhr.status<500&&![408,429].includes(xhr.status)});end(null,result);}catch(error){end(error);}};
    xhr.onerror=()=>end(failure('资料连接中断，原文件和编号保留 / 자료 연결이 끊겼습니다',{retryable:true}));
    xhr.ontimeout=()=>end(failure('资料上传等待超时，文件可能已到达服务器，原编号保留',{retryable:true}));
    xhr.onabort=()=>end(failure(timedOut?'资料传输长时间没有进展，原文件和编号保留':'上传已停止，原文件和编号保留',{retryable:timedOut,cancelled:!timedOut}));
    signal?.addEventListener('abort',stop,{once:true});touch();progress?.(0,file.size,{attempt:number,phase:number>1?'retrying':'uploading'});xhr.send(captured);
   }catch(error){end(error);}
  }));
  for(let n=1;;n++){check();try{return await attempt(n);}catch(error){check();if(!error.retryable||n>=limits.attempts)throw error;progress?.(0,file.size,{attempt:n+1,phase:'retrying'});await new Promise((resolve,reject)=>{let timer;const stop=()=>{clearTimeout(timer);signal?.removeEventListener('abort',stop);reject(failure('上传已停止，原文件和编号保留',{cancelled:true}));};timer=setTimeout(()=>{signal?.removeEventListener('abort',stop);resolve();},limits.retryMs[n-1]??1500);if(signal?.aborted)stop();else signal?.addEventListener('abort',stop,{once:true});});}}
 }
 async function files(items,run,{concurrency=1}={}){items.filter(item=>!item.uploaded&&!item.done&&!item.cancelled).forEach(item=>validate(item.file));const check=identity();let next=0,error=null;await Promise.all(Array.from({length:Math.min(items.length,Math.max(1,Math.min(2,concurrency)))},async()=>{while(!error){const i=next++;if(i>=items.length)return;try{check();await run(items[i],i);check();}catch(e){error||=e;}}}));if(error)throw error;}
 function status(loaded,size,info={}){return (info.phase==='retrying'?'连接中断，正在重试 / 다시 시도 ':'正在上传 / 업로드 중 ')+Math.min(100,Math.round(loaded/Math.max(1,size)*100))+'%'+(info.attempt>1?' · '+info.attempt+'/'+limits.attempts:'');}
 window.CKMaterialUpload={send,files,validate,identity,status,limits};
})();
