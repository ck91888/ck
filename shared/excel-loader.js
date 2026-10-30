// Only the staging collaboration build installs this loader. Other entrances
// retain their existing eager library and all callers keep that fallback.
(function(){
 'use strict';
 const source=document.currentScript.getAttribute('data-excel-src');
 let pending=null;
 const ready=()=>window.XLSX?.read&&window.XLSX?.utils&&window.XLSX?.writeFile;
 window.CKEnsureExcel=function(){
  if(ready())return Promise.resolve(window.XLSX);
  if(pending)return pending;
  pending=new Promise((resolve,reject)=>{
   const script=document.createElement('script');let finished=false;
   const finish=error=>{
    if(finished)return;finished=true;clearTimeout(timer);script.onload=script.onerror=null;
    if(error){script.remove();reject(error);}else resolve(window.XLSX);
   };
   const unavailable=()=>Error('表格组件加载失败，请检查网络后重试本次操作 / Excel 구성 요소를 불러오지 못했습니다. 다시 시도하세요.');
   const timer=setTimeout(()=>finish(unavailable()),60000);
   script.src=source;script.async=true;
   script.onload=()=>finish(ready()?null:unavailable());script.onerror=()=>finish(unavailable());
   document.head.append(script);
  }).catch(error=>{pending=null;throw error;});
  return pending;
 };
})();
