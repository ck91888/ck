(function(){
'use strict';
window.CKInstallLoadTrip=function(){
 if(!window.CK_SOP_ROLLOUT?.staging)return;
 const $=id=>document.getElementById(id),esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const key='ck_load:'+CKSession.user.id+':',read=k=>{try{return JSON.parse(sessionStorage.getItem(key+k)||'null');}catch{return null;}},write=(k,v)=>sessionStorage.setItem(key+k,JSON.stringify(v)),drop=k=>sessionStorage.removeItem(key+k);
 const request=async b=>{const r=await api(b);if(!r?.ok)throw Object.assign(Error(r?.message||r?.error||'请求失败 / 요청 실패'),{businessError:!r?.retryable&&!r?.network_error});return r;};
 const modes={warehouse_dispatch:'仓库叫车 / 창고 배차',customer_pickup:'客户自提 / 고객 픽업',milk_express:'牛奶快递 / 밀크런 택배',milk_pallet:'牛奶托盘 / 밀크런 팔레트',container_pickup:'柜子提货 / 컨테이너 픽업'};
 const name=o=>o.display_no||o.wms_work_order_no||'单号待补充',summary=x=>`<strong>${esc(name(x.order))}</strong><span>客户 / 고객：${esc(x.order.customer||'—')}</span><span>${esc(modes[x.order.outbound_mode]||x.order.outbound_mode||'—')}</span><span>应装 / 예정：${x.planned_box_count} 箱 / 박스 · ${x.planned_pallet_count} 托 / 팔레트</span>`;
 let selected=new Map(read('selection')||[]),candidates=[],offset=0,version=0,queue=Promise.resolve(),pendingScans=0,starting=false,finishing=false,scanner=null,lastScan='',lastScanAt=0,detail=null,epoch=0;
 let pendingStart=read('start'),resume=read('resume');
 const page=$('page-outbound_load'),entry=document.createElement('section'),working=document.createElement('section');
 entry.id='ck-load-entry';entry.className='card ck-load-trip';working.id='ck-load-working';working.className='ck-load-trip';working.hidden=true;
 entry.innerHTML=`<h3>本车出库清单 / 차량 출고 목록</h3><form id="ck-load-scan"><label for="ck-load-code">扫描或输入出库单号 / 출고번호 스캔</label><div class="ck-load-row"><input id="ck-load-code" autocomplete="off" placeholder="连续扫码，逐单加入 / 연속 스캔"><button type="submit">加入 / 추가</button></div></form>
 <button type="button" id="ck-load-camera">相机连续扫码 / 카메라 연속 스캔</button><div id="ck-load-reader"></div><p id="ck-load-message" role="status" aria-live="polite"></p><h4 id="ck-load-count"></h4><div id="ck-load-selected"></div>
 <details id="ck-load-lookup"><summary>从待出库列表多选 / 출고 목록 선택</summary><form id="ck-load-search" class="ck-load-row"><input aria-label="搜索客户或出库单号" placeholder="客户、出库单号 / 고객·출고번호"><button type="submit">查找 / 검색</button></form><div id="ck-load-candidates"></div><div class="ck-load-row"><button type="button" id="ck-load-prev">上一页 / 이전</button><button type="button" id="ck-load-next">下一页 / 다음</button></div></details>
 <div class="ck-load-grid"><label>本车车牌（选填） / 차량번호<input id="ck-load-vehicle" maxlength="50"></label><label>本车司机（选填） / 운전자<input id="ck-load-driver" maxlength="100"></label></div><button type="button" id="ck-load-start" class="btn btn-success">分配人员并开始本车装货 / 인원 배정·상차 시작</button><button type="button" id="ck-load-temporary" class="btn btn-outline">无单临时装货 / 서류없이 임시 상차</button>`;
 page.append(entry,working);
 const legacy=['obLoadEntryCard','obLoadActionCard','loadResultCard','loadInterruptBar'];
 const workerCard=$('loadWorkers').closest('.card');
 function hideLegacy(){for(const id of legacy)$(id).style.display='none';workerCard.hidden=true;}
 function message(s,bad=false){$('ck-load-message').textContent=s;$('ck-load-message').classList.toggle('ck-load-error',bad);}
 const storeSelection=()=>write('selection',[...selected]);
 function controls(){
  const locked=starting||!!pendingStart;
  entry.querySelectorAll('input,button').forEach(e=>e.disabled=locked||e.hasAttribute('data-blocked'));
  $('ck-load-start').disabled=starting||pendingScans>0||(!pendingStart&&(!selected.size||[...selected.values()].some(x=>x.reason)));
  $('ck-load-start').textContent=pendingStart?'重试本次派工 / 배정 재시도':'分配人员并开始本车装货 / 인원 배정·상차 시작';
  $('ck-load-prev').disabled=locked||offset===0;$('ck-load-next').disabled=locked||!entry.dataset.more;
 }
 function renderSelected(){
  $('ck-load-count').textContent='已选 '+selected.size+' 单 / '+selected.size+'개 선택';
  $('ck-load-selected').innerHTML=[...selected.values()].map(x=>`<article class="ck-load-order"><div class="ck-load-summary">${summary(x)}</div>${x.reason?'<p class="ck-load-error">'+esc(x.reason)+'</p>':''}<div class="ck-load-row"><button type="button" data-inspect="${esc(x.order.id)}">查看单据 / 확인</button><button type="button" data-remove="${esc(x.order.id)}">移除 / 제외</button></div><div data-order-detail="${esc(x.order.id)}"></div></article>`).join('')||'<p>尚未选择出库单 / 출고단을 선택하세요</p>';
  $('ck-load-candidates').querySelectorAll('[data-select]').forEach(c=>c.checked=selected.has(c.value));controls();
 }
 function add(x){
  if(selected.has(x.order.id)){message(name(x.order)+' 已在本车清单中，不会重复加入 / 이미 추가됨');return;}
  if(selected.size>=20)throw Error('本车最多20张出库单 / 최대 20개');
  if(x.reason){message(name(x.order)+'：'+x.reason,true);return;}
  selected.set(x.order.id,x);storeSelection();renderSelected();message('已加入 '+name(x.order)+' / 추가 완료');
 }
 function enqueue(code){
  code=String(code||'').trim();if(!code||starting||pendingStart)return;
  pendingScans++;controls();
  queue=queue.then(async()=>{const x=await request({action:'v2_outbound_order_resolve_code',scene:'load_trip',code});add(x);if(x.reason)await showBlocked(x);}).catch(e=>message(e.message,true)).finally(()=>{pendingScans--;controls();});
 }
 $('ck-load-scan').onsubmit=e=>{e.preventDefault();const v=$('ck-load-code').value;$('ck-load-code').value='';enqueue(v);$('ck-load-code').focus();};
 function remove(id){if(starting||pendingStart)return;selected.delete(id);storeSelection();renderSelected();message('已从本车移除 / 차량 목록에서 제외');}
 $('ck-load-selected').onclick=e=>{const b=e.target.closest('button');if(!b||starting||pendingStart)return;if(b.dataset.remove)remove(b.dataset.remove);if(b.dataset.inspect)inspect(b.dataset.inspect,b.closest('article').querySelector('[data-order-detail]'));};
 async function loadList(){
  const n=++version;try{const r=await request({action:'v2_outbound_order_list',scene:'load_trip',search:$('ck-load-search').querySelector('input').value.trim(),offset});if(n!==version)return;
   candidates=r.items;entry.dataset.more=r.more?'1':'';
   $('ck-load-candidates').innerHTML=candidates.map(x=>`<article class="ck-load-order"><label class="ck-load-option"><input type="checkbox" data-select="${esc(x.order.id)}" value="${esc(x.order.id)}" ${selected.has(x.order.id)?'checked':''} ${x.reason?'disabled data-blocked':''}><span class="ck-load-summary">${summary(x)}${x.reason?'<span class="ck-load-error">'+esc(x.reason)+'</span>':''}</span></label><button type="button" data-inspect="${esc(x.order.id)}">查看单据 / 확인</button><div data-order-detail="${esc(x.order.id)}"></div></article>`).join('')||'<p>暂无匹配的出库单 / 일치하는 출고단 없음</p>';controls();
  }catch(e){message(e.message,true);}
 }
 $('ck-load-candidates').onchange=e=>{const x=candidates.find(x=>x.order.id===e.target.value);if(!x)return;if(starting||pendingStart){e.target.checked=selected.has(x.order.id);return;}if(e.target.checked)add(x);else remove(x.order.id);};
 $('ck-load-candidates').onclick=e=>{const b=e.target.closest('[data-inspect]');if(b)inspect(b.dataset.inspect,b.closest('article').querySelector('[data-order-detail]'));};
 $('ck-load-search').onsubmit=e=>{e.preventDefault();offset=0;loadList();};
 $('ck-load-lookup').ontoggle=()=>{if($('ck-load-lookup').open)loadList();};
 $('ck-load-prev').onclick=()=>{offset=Math.max(0,offset-50);loadList();};$('ck-load-next').onclick=()=>{offset+=50;loadList();};
 async function showBlocked(x){const host=$('ck-load-candidates');$('ck-load-lookup').open=true;await loadList();let card=[...host.querySelectorAll('article')].find(a=>a.querySelector('[data-inspect]')?.dataset.inspect===x.order.id);
  if(!card){card=document.createElement('article');card.className='ck-load-order';card.innerHTML='<div class="ck-load-summary">'+summary(x)+'</div><div data-order-detail></div>';host.prepend(card);}await inspect(x.order.id,card.querySelector('[data-order-detail]'));
 }
 async function inspect(id,host){
  host.textContent='读取最新单据 / 확인 중';try{const r=await request({action:'v2_outbound_order_detail',id});if(!host.isConnected)return;const o=r.order;
   const logs=(r.pending_change_logs||[]).map(l=>'<p>'+esc(l.summary_text||'出库单已更新')+'</p>'+renderOutboundDiffTable001(l.diff)).join('');
   host.innerHTML=`<p>PO：${esc(o.po_no||'—')}</p><p>备注 / 비고：${esc(o.remark||'—')}</p><p>提货 / 픽업：${esc([o.pickup_company,o.pickup_person_name,o.pickup_driver_name,o.pickup_driver_phone,o.pickup_vehicle_no,o.pickup_time,o.pickup_note].filter(Boolean).join(' · ')||'—')}</p>${logs}${Number(o.warehouse_ack_required)?'<button type="button" data-ack>已查看并确认此单变更 / 변경 확인</button>':''}${Number(o.pickup_confirm_required)?'<button type="button" data-pickup>已查看并确认提货信息 / 픽업 확인</button>':''}`;
   for(const button of host.querySelectorAll('button'))button.onclick=async()=>{button.disabled=true;try{await request({action:button.hasAttribute('data-ack')?'v2_outbound_order_ack_change':'v2_outbound_pickup_confirm',id,revision_no:Number(o.revision_no||0),client_req_id:crypto.randomUUID()});await inspect(id,host);await refreshSelected();if(detail)await refreshWorking();else await loadList();}catch(e){const p=document.createElement('p');p.className='ck-load-error';p.textContent=e.message;host.append(p);}finally{button.disabled=false;}};
  }catch(e){host.textContent=e.message;}
 }
 async function refreshSelected(){
  const values=[...selected.values()];for(const x of values){try{const r=await request({action:'v2_outbound_order_resolve_code',scene:'load_trip',code:x.order.id});if(selected.has(x.order.id))selected.set(x.order.id,r);}catch(e){if(selected.has(x.order.id))selected.set(x.order.id,{...x,reason:e.message});}}storeSelection();renderSelected();
 }
 async function stopCamera(){const old=scanner;scanner=null;if(old){await old.stop().catch(()=>{});try{old.clear();}catch{}}$('ck-load-camera').textContent='相机连续扫码 / 카메라 연속 스캔';}
 $('ck-load-camera').onclick=async()=>{if(scanner){await stopCamera();return;}try{const s=new Html5Qrcode('ck-load-reader');scanner=s;await startManagedQrScanner(s,'ck-load-reader',{fps:10,qrbox:{width:250,height:150}},code=>{const t=Date.now();if(code===lastScan&&t-lastScanAt<2000)return;lastScan=code;lastScanAt=t;enqueue(code);},()=>{});$('ck-load-camera').textContent='停止相机 / 카메라 중지';}catch(e){await stopCamera();message('相机启动失败 / 카메라 오류：'+e.message,true);}};
 $('ck-load-start').onclick=async()=>{
  if(starting||pendingScans)return;starting=true;controls();await stopCamera();
  try{
   if(!pendingStart){const payload={action:'v2_outbound_load_start',order_ids:[...selected.keys()],driver_name:$('ck-load-driver').value.trim(),vehicle_no:$('ck-load-vehicle').value.trim(),client_req_id:crypto.randomUUID()};
    const staff=await CKChooseNativeStaff(payload);if(!staff)return;pendingStart={payload,...staff};write('start',pendingStart);controls();
   }
   const r=await CKSession.request('sop_native_start',pendingStart);CKSetNativeLead(r.lead);drop('start');pendingStart=null;selected.clear();storeSelection();
   saveActiveJob(r.job_id,null);write('resume',{id:r.job_id});await showWorking(await request({action:'v2_ops_job_detail',job_id:r.job_id}));
  }catch(e){if(e.businessError){drop('start');pendingStart=null;await refreshSelected();}message(e.message+(pendingStart?'；保留本次派工，网络恢复后点击重试 / 연결 복구 후 재시도':''),true);}finally{starting=false;controls();}
 };
 // The existing no-document operation retains its original result and history path.
 $('ck-load-temporary').onclick=async()=>{await stopCamera();const r=await request({action:'v2_outbound_load_start'});saveActiveJob(r.job_id,r.worker_seg_id);await window.initOutboundLoad();};
 const originalInit=window.initOutboundLoad;
 window.initOutboundLoad=async function(){
  const n=++epoch;hideLegacy();entry.hidden=true;working.hidden=true;detail=null;
  try{if(_activeJobId){const id=_activeJobId,r=await request({action:'v2_ops_job_detail',job_id:id});if(n!==epoch||_currentPage!=='outbound_load')return;
    if(['completed','cancelled'].includes(r.job.status)){exit(id);return;}
    if(r.job.job_type==='load_outbound'&&r.load_orders?.length){await showWorking(r);return;}
    entry.hidden=true;workerCard.hidden=false;return originalInit();
   }
   entry.hidden=false;renderSelected();if(pendingStart){$('ck-load-driver').value=pendingStart.payload.driver_name||'';$('ck-load-vehicle').value=pendingStart.payload.vehicle_no||'';message('上次派工响应未确认，请重试同一次请求 / 이전 배정 재시도',true);}else{$('ck-load-code').focus();await refreshSelected();}
  }catch(e){entry.hidden=false;message(e.message,true);}
 };
 function draftKey(id){return 'draft:'+id;}
 function saveDraft(){if(!detail||finishing)return;const form=working.querySelector('form');if(!form)return;write(draftKey(detail.job.id),{values:[...form.querySelectorAll('[data-result]')].map(row=>({id:row.dataset.result,box:row.querySelector('[name=box]').value,pallet:row.querySelector('[name=pallet]').value})),remark:form.elements.remark.value});}
 async function showWorking(r){
  detail=r;hideLegacy();entry.hidden=true;working.hidden=false;write('resume',{id:r.job.id});
  const draft=read(draftKey(r.job.id)),pending=read('finish:'+r.job.id),results=pending?.order_results;
  working.innerHTML=`<section class="card"><h3>本车装货 · ${r.load_orders.length} 单 / 차량 상차</h3><p>${esc([r.load_trip?.vehicle_no,r.load_trip?.driver_name].filter(Boolean).join(' · '))}</p><p data-working-crew>${esc(r.workers.filter(w=>!w.left_at).map(w=>w.worker_name).join('、'))||'暂无人员 / 없음'}</p><button type="button" data-refresh>刷新单据 / 새로고침</button></section><form><div data-orders>${r.load_orders.map(x=>{
   const saved=draft?.values?.find(v=>v.id===x.order.id),frozen=results?.find(v=>v.order_id===x.order.id);
   return `<section class="card ck-load-order" data-result="${esc(x.order.id)}"><div class="ck-load-summary">${summary(x)}</div><p class="ck-load-error" data-reason>${esc(x.reason)}</p><button type="button" data-inspect="${esc(x.order.id)}">查看单据 / 확인</button><div data-order-detail></div><div class="ck-load-grid"><label>实装箱数 / 실제 박스<input name="box" type="number" min="0" max="100000000" step="1" inputmode="numeric" value="${esc(frozen?.box_count??saved?.box??'')}" placeholder="${x.planned_box_count}"></label><label>实装托数 / 실제 팔레트<input name="pallet" type="number" min="0" max="100000000" step="1" inputmode="numeric" value="${esc(frozen?.pallet_count??saved?.pallet??'')}" placeholder="${x.planned_pallet_count}"></label></div><button type="button" data-planned>按应装数量填入 / 예정 수량 입력</button></section>`;
  }).join('')}</div><section class="card"><label>本车备注 / 비고<textarea name="remark" rows="2">${esc(pending?.remark??draft?.remark??'')}</textarea></label><p data-finish-message role="alert"></p><button class="btn btn-success" type="submit">${pending?'重试本次结束 / 완료 재시도':'逐单核对并结束本车装货 / 확인 후 상차 종료'}</button><button type="button" data-home>返回首页，任务继续 / 작업 유지·홈으로</button></section></form><section class="card"><h4>提货车辆照片 / 차량 사진</h4><button type="button" data-camera>拍照 / 촬영</button><button type="button" data-album>从相册选择 / 앨범 선택</button><div data-photos></div></section>`;
  const form=working.querySelector('form');if(pending)form.querySelectorAll('input,textarea,[data-planned]').forEach(x=>x.disabled=true);
  form.oninput=saveDraft;
  working.querySelector('[data-refresh]').onclick=refreshWorking;
  working.querySelector('[data-home]').onclick=()=>goPage('home');
  working.querySelector('[data-camera]').onclick=()=>uploadPhoto('ops_job','vehicle_photo','camera');working.querySelector('[data-album]').onclick=()=>uploadPhoto('ops_job','vehicle_photo','album');
  working.querySelector('[data-photos]').innerHTML=(r.attachments||[]).filter(a=>a.attachment_category==='vehicle_photo').map(a=>`<img class="photo-thumb" alt="${esc(a.file_name)}" src="${esc(fileUrl(a.file_key))}">`).join('');
  for(const row of form.querySelectorAll('[data-result]')){
   row.querySelector('[data-planned]').onclick=()=>{const x=detail.load_orders.find(x=>x.order.id===row.dataset.result);row.querySelector('[name=box]').value=x.planned_box_count;row.querySelector('[name=pallet]').value=x.planned_pallet_count;saveDraft();};
   row.querySelector('[data-inspect]').onclick=()=>inspect(row.dataset.result,row.querySelector('[data-order-detail]'));
  }
  form.onsubmit=async e=>{e.preventDefault();if(finishing)return;saveDraft();finishing=true;const button=form.querySelector('[type=submit]'),hint=form.querySelector('[data-finish-message]');button.disabled=true;
   try{let b=read('finish:'+r.job.id);if(!b){b={action:'v2_outbound_load_finish',job_id:r.job.id,worker_id:getWorkerId(),complete_job:true,client_req_id:crypto.randomUUID(),remark:form.elements.remark.value.trim(),order_results:[...form.querySelectorAll('[data-result]')].map(row=>({order_id:row.dataset.result,box_count:Number(row.querySelector('[name=box]').value||0),pallet_count:Number(row.querySelector('[name=pallet]').value||0)}))};
     if(!r.load_trip&&b.order_results.length===1)Object.assign(b,b.order_results[0]);write('finish:'+r.job.id,b);}
    await request(b);exit(r.job.id);
   }catch(e){if(e.businessError){drop('finish:'+r.job.id);form.querySelectorAll('input,textarea,[data-planned]').forEach(x=>x.disabled=false);}else{form.querySelectorAll('input,textarea,[data-planned]').forEach(x=>x.disabled=true);button.textContent='重试本次结束 / 완료 재시도';}hint.textContent=e.message;}finally{finishing=false;button.disabled=false;}
  };
 }
 async function refreshWorking(){
  if(!detail||finishing)return;const id=detail.job.id;saveDraft();try{const r=await request({action:'v2_ops_job_detail',job_id:id});if(_activeJobId!==id||_currentPage!=='outbound_load')return;if(['completed','cancelled'].includes(r.job.status)){exit(id);return;}await showWorking(r);}catch(e){const hint=working.querySelector('[data-finish-message]');if(hint)hint.textContent=e.message;}
 }
 function exit(id){drop(draftKey(id));drop('finish:'+id);drop('resume');detail=null;CKClearNativeJob();window._navStack=[];goPage('home');}
 window.addEventListener('ck-native-people-changed',e=>{if(detail?.job.id!==e.detail?.job.id||_currentPage!=='outbound_load')return;saveDraft();showWorking(e.detail);});
 const originalPage=window.showPage;window.showPage=function(...args){if(args[0]!=='outbound_load'){epoch++;saveDraft();stopCamera();drop('resume');}return originalPage(...args);};
 const originalPhotos=window.handlePhotoUpload;window.handlePhotoUpload=async function(...args){await originalPhotos(...args);if(_currentPage==='outbound_load'&&detail)await refreshWorking();};
 hideLegacy();renderSelected();
 if(resume?.id){saveActiveJob(resume.id,null);goPage('outbound_load');}
};
})();
