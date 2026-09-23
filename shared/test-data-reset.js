(function(){
 'use strict';
 if(!window.CK_SOP_ROLLOUT?.staging)return;
 const ready=document.readyState==='loading'?new Promise(r=>document.addEventListener('DOMContentLoaded',r,{once:true})):Promise.resolve();
 Promise.all([ready,window.CKSession.ready]).then(async()=>{
  const host=document.querySelector('.ck-home');if(!host||window.CKSession.user?.role!=='manager')return;
  const section=document.createElement('section');section.className='ck-test-tools';
  section.innerHTML='<div><h2>测试管理 <small>테스트 관리</small></h2><p>重新开始一轮测试，保留账号和基础设置。</p></div><button type="button" class="ck-reset-open">清空测试数据 <span>테스트 데이터 삭제</span></button>';
  host.append(section);
  const dialog=document.createElement('dialog');dialog.className='ck-reset-dialog';dialog.setAttribute('aria-labelledby','ck-reset-title');
  dialog.innerHTML='<h2 id="ck-reset-title">清空测试数据</h2><p class="ck-reset-korean">테스트 데이터 삭제</p><p>清空当前测试站的全部业务记录，再上传新数据测试。<br><small>업무 기록을 삭제하고 새 데이터로 테스트합니다.</small></p><dl class="ck-reset-counts"></dl><p class="ck-reset-scope">同时清空作业需求、职员资料、日当及职员工牌与工时、核对、导入、耗材与物品记录。<br><small>작업 요청·직원 정보·명찰·근무시간·검수·가져오기·자재 기록 포함</small></p><p class="ck-reset-keep">保留：账号、人力公司选项、库位和系统设置。<br><small>계정·인력회사·창고 위치·설정 유지</small></p><p class="ck-reset-note">清空前自动备份；清理期间请暂勿操作其他页面。</p><progress hidden max="100" value="0" aria-label="清理进度 / 삭제 진행률"></progress><p class="ck-reset-status" role="status" aria-live="polite"></p><div class="ck-reset-actions"><button type="button" data-close>取消 / 취소</button><button type="button" data-confirm>确认清空 / 삭제 확인</button></div>';
  document.body.append(dialog);
  const open=section.querySelector('button'),confirm=dialog.querySelector('[data-confirm]'),close=dialog.querySelector('[data-close]'),status=dialog.querySelector('[role=status]'),progress=dialog.querySelector('progress'),countList=dialog.querySelector('dl');
  let run=null,busy=false,requestId='',failed=false;
  async function api(action,body){const r=await fetch('/api/test-data/'+action,{method:body?'POST':'GET',credentials:'same-origin',headers:body?{'Content-Type':'application/json','X-CK-Test-Reset':'1'}:{},body:body?JSON.stringify(body):undefined});const d=await r.json();if(!r.ok||!d.ok)throw Error(d.error||'请求失败 / 요청 실패');return d;}
  function renderRun(){if(!run)return;progress.hidden=false;progress.value=run.total?run.done/run.total*100:0;if(run.completedAt){status.textContent='已清空，可以上传新数据了。 / 삭제 완료. 새 데이터를 업로드하세요.';confirm.hidden=true;close.textContent='完成 / 완료';}else{status.textContent='正在备份并清理 '+run.done+' / '+run.total+' · 백업 및 삭제 중';confirm.textContent='继续清理 / 계속 삭제';}}
  function lock(value){busy=value;confirm.disabled=value;close.disabled=value;open.disabled=value;dialog.setAttribute('aria-busy',String(value));}
  async function refresh(){const d=await api('status');run=d.run&&!d.run.completedAt?d.run:null;countList.replaceChildren();const counts=Object.fromEntries(d.counts.map(x=>[x.table_name,x.count]));for(const [label,key] of [['入库计划 / 입고','v2_inbound_plans'],['出库计划 / 출고','v2_outbound_orders'],['现场任务 / 현장 작업','v2_ops_jobs'],['签到记录 / 출근 기록','ck_attendance_days']]){const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=label;dd.textContent=counts[key]??'—';countList.append(dt,dd);}if(run)renderRun();else status.textContent='';}
  open.onclick=async()=>{failed=false;run=null;requestId=crypto.randomUUID();confirm.hidden=false;confirm.textContent='确认清空 / 삭제 확인';close.textContent='取消 / 취소';progress.hidden=true;status.textContent='正在读取数据 / 불러오는 중';dialog.showModal();lock(true);try{await refresh();}catch(e){status.textContent=e.message;failed=true;}finally{lock(false);if(failed)confirm.disabled=true;}};
  confirm.onclick=async()=>{lock(true);try{if(!run)run=(await api('start',{requestId,confirmation:'CLEAR_TEST_DATA'})).run;renderRun();while(!run.completedAt){run=(await api('step',{runId:run.id})).run;renderRun();}}catch(e){status.textContent='清理暂停：'+e.message+' / 삭제가 일시 중단되었습니다';confirm.textContent='重试 / 다시 시도';}finally{lock(false);}};
  close.onclick=()=>{if(!busy)dialog.close();};dialog.addEventListener('cancel',e=>{if(busy)e.preventDefault();});
  // A reload never starts a fresh reset. It only offers to resume an active run.
  try{const d=await api('status');if(d.run&&!d.run.completedAt){open.textContent='继续清理测试数据 / 삭제 계속';}}catch{}
 });
})();
