(function(){
 'use strict';
 const ready=import('/shared/internal-operations.js');
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const hosts=new Map(),jobs=new Map();
 function host(id,before){
  if(hosts.has(id))return hosts.get(id);
  const el=document.createElement('section');el.id=id;el.className='card ck-internal-operation';before.before(el);el.addEventListener('input',()=>{const message=el.querySelector('[role=alert]');if(message)message.textContent='';const generic=document.getElementById('gjErrorBar');if(generic){generic.textContent='';generic.style.display='none';}});hosts.set(id,el);return el;
 }
 function fields(r,type,mode,phase){
  return r.operationFields(type,mode,phase).map(key=>{
   const label=esc(r.metricLabels[key]);
   if(key==='used_forklift')return '<fieldset><legend>'+label+'</legend><label><input type="radio" name="'+phase+'-used_forklift" data-operation-field="'+key+'" value="yes"> 是 / 예</label> <label><input type="radio" name="'+phase+'-used_forklift" data-operation-field="'+key+'" value="no"> 否 / 아니요</label></fieldset>';
   return '<label class="form-group">'+label+(key==='organizing_content'?'<textarea data-operation-field="'+key+'" maxlength="2000" rows="3"></textarea>':'<input data-operation-field="'+key+'" type="number" min="0" max="100000000" step="1" inputmode="numeric" placeholder="必填，允许 0 / 필수, 0 가능">')+'</label>';
  }).join('');
 }
 function values(el){
  const out={};
  for(const input of el.querySelectorAll('[data-operation-field]')){const key=input.dataset.operationField;if(input.type==='radio'){if(input.checked)out[key]=input.value==='yes';}else out[key]=input.value;}
  return out;
 }
 window.CKInternalOperations={
  async idle(type){
   const r=await ready,el=host('ck-internal-start',document.querySelector('#gjStateIdle>.card:nth-child(2)'));
   el.hidden=!r.isInternal(type);el.dataset.type=type;el.innerHTML='';
   if(el.hidden)return;
   el.innerHTML=(type==='inventory'?'<label>盘点类型 / 재고조사 종류<select data-inventory-type><option value="">请选择 / 선택하세요</option>'+Object.entries(r.inventoryLabels).map(([k,v])=>'<option value="'+k+'">'+esc(v)+'</option>').join('')+'</select></label>':'')+'<p data-timing></p><div data-start-fields></div>';
   const render=()=>{const mode=el.querySelector('[data-inventory-type]')?.value;
    el.querySelector('[data-timing]').textContent=type==='inventory'?mode==='precise'?'精盘：开始时填写数量、货位及叉车；结束无需重复填写。 / 정밀 조사: 시작 시 입력':mode==='blind'?'盲盘：结束时填写数量、货位及叉车。 / 블라인드 조사: 종료 시 입력':'先选择盲盘或精盘 / 종류를 선택하세요':type==='disposal'?'开始时填写拣货数量、货位及叉车 / 시작 시 입력':'结束时填写实际结果 / 종료 시 실제 결과 입력';
    el.querySelector('[data-start-fields]').innerHTML=mode||type!=='inventory'?fields(r,type,mode,'start'):'';
   };el.querySelector('select')?.addEventListener('change',render);render();
  },
  async start(type){
   const r=await ready;if(!r.isInternal(type))return {};
   const el=hosts.get('ck-internal-start'),mode=el?.querySelector('select')?.value;
   const body={...(type==='inventory'?{inventory_type:mode}:{}),internal_metrics:values(el)};
   r.validateOperationStart(type,body);return body;
  },
  async working(job){
   const r=await ready;if(!job||!r.isInternal(job.job_type)||job.id!==window._activeJobId)return;
   const returned=job.job_type==='inbound_return',id=returned?'ck-return-finish':'ck-internal-finish';
   const el=host(id,returned?document.getElementById('inboundReturnResultNote').closest('.card'):document.getElementById('gjErrorBar'));
   jobs.set(job.id,job);const d=r.readOperation(job);
   if(el.dataset.jobId!==job.id){el.dataset.jobId=job.id;el.innerHTML='<p data-operation-summary></p><div data-finish-fields></div><p role="alert"></p>';el.querySelector('[data-finish-fields]').innerHTML=d?fields(r,job.job_type,d.inventory_type,'finish'):'<p>沿用旧任务完成流程；未记录的类型和开始数据不补推。 / 기존 작업 방식으로 완료하며 미기록 정보를 추정하지 않습니다.</p>';}
   el.hidden=false;el.querySelector('[data-operation-summary]').textContent=r.operationSummary(job);
  },
  async finish(id){
   const r=await ready;let job=jobs.get(id);
   if(!job){const detail=await window.api({action:'v2_ops_job_detail',job_id:id});if(!detail?.ok)throw Error(detail?.error||'请重新打开任务 / 작업을 다시 여세요');job=detail.job;await this.working(job);}
   if(!r.isInternal(job.job_type)||!r.readOperation(job))return {};
   const el=hosts.get(job.job_type==='inbound_return'?'ck-return-finish':'ck-internal-finish'),input={internal_metrics:values(el)};
   try{r.validateOperationFinish(job,input);el.querySelector('[role=alert]').textContent='';return input;}
   catch(e){el.querySelector('[role=alert]').textContent=e.message;throw e;}
  }
 };
})();
