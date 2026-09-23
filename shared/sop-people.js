(function(){
 'use strict';
 // One selection model for linked work and the original business start dialog.
 function selection(initial=[],initialLead=''){
  let people=initial.map(p=>({...p})),lead=initialLead,requireLead=false;
  if(!people.some(p=>p.id===lead))lead=people[0]?.id||'';
  return {
   get people(){return people.map(p=>({...p}));},get lead(){return lead;},
   select(id){lead=people.some(p=>p.id===id)?id:'';},
   add(raw){
    const parts=String(raw||'').trim().split('|').map(p=>p.trim());
    if(parts.length!==2||!parts[0]||!parts[1])throw Error('工牌格式应为 工号|姓名 / 사번|이름');
    const [id,name]=parts,existing=people.find(p=>p.id===id);
    if(existing)return {added:false,message:'工号 '+id+' 已添加（'+existing.name+'），未重复加入 / 이미 추가된 사번'+(existing.name!==name?'；姓名不一致，请核对工牌':'')};
    people.push({id,name});if(!lead&&!requireLead)lead=id;
    return {added:true,message:'已添加 '+name+'（'+id+'） / 추가 완료'};
   },
   remove(id){people=people.filter(p=>p.id!==id);if(lead===id){lead='';requireLead=true;return '已移除主操作员，请重新选择 / 주 작업자를 다시 선택하세요';}return '已移除该人员 / 작업자 삭제 완료';},
   read(){if(!people.length)throw Error('至少登记一名操作人员 / 작업자를 추가하세요');if(!people.some(p=>p.id===lead))throw Error('请选择主操作员 / 주 작업자를 선택하세요');return {workers:people.map(p=>({...p})),lead_id:lead};}
  };
 }
 window.CKPeopleSelection=selection;
 window.CKPeopleFields=function(){return '<label>扫描工牌 / 명찰<input data-staff-badge placeholder="TEST-A|测试操作员甲" autocomplete="off"></label><div class="toolbar"><button type="button" data-staff-add>添加工牌</button><button type="button" data-staff-camera>相机扫码 / 카메라</button></div><div data-staff-scanner></div><p data-staff-count></p><p data-staff-status role="status" aria-live="polite"></p><div data-staff-people></div><label>主操作员 / 주 작업자<select name="lead_id" data-staff-lead required></select></label>';};
 let scannerSequence=0;
 window.CKPeoplePicker=function(root,{workers=[],leadId='',error,allowEmpty=false}={}){
  const find=s=>root.querySelector('[data-staff-'+s+']'),input=find('badge'),lead=find('lead'),status=find('status'),camera=find('camera'),model=selection(workers,leadId);
  const scannerBox=find('scanner');scannerBox.id='ck-staff-scanner-'+(++scannerSequence);
  let scanner=null,starting=false,closed=false,lastCamera='',lastCameraAt=0;
  const report=message=>{status.textContent=message;};
  const showError=e=>{report(e.message);if(error)error.textContent=e.message;};
  function render(){
   find('people').replaceChildren();lead.replaceChildren(new Option('请选择主操作员 / 주 작업자 선택',''));
   for(const p of model.people){const item=document.createElement('span');item.className='badge';item.textContent=p.name+' ('+p.id+') ';const remove=document.createElement('button');remove.type='button';remove.className='light';remove.textContent='移除';remove.setAttribute('aria-label','移除 '+p.name+' ('+p.id+')');remove.onclick=()=>{model.select(lead.value);report(model.remove(p.id));if(error)error.textContent='';render();};item.append(remove);find('people').append(item);lead.add(new Option(p.name+' ('+p.id+')',p.id));}
   lead.required=!allowEmpty||model.people.length>0;lead.value=model.lead;find('count').textContent='已选 '+model.people.length+' 人 / 선택 '+model.people.length+'명';
  }
  function add(raw){if(closed)return;try{model.select(lead.value);const result=model.add(raw);render();report(result.message);if(error)error.textContent='';input.value='';input.focus();}catch(e){showError(e);input.focus();}}
  find('add').onclick=()=>add(input.value);input.onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();add(input.value);}};lead.onchange=()=>{model.select(lead.value);if(error)error.textContent='';};
  async function stop(){const current=scanner;scanner=null;if(current)await current.stop().catch(()=>{});camera.textContent='相机扫码 / 카메라';}
  find('camera').onclick=async()=>{
   if(starting)return;if(scanner){await stop();return;}
   starting=true;const current=new Html5Qrcode(scannerBox.id);scanner=current;
   try{await current.start({facingMode:'environment'},{fps:8,qrbox:220},raw=>{if(closed||scanner!==current)return;const key=String(raw).trim(),now=Date.now();if(key===lastCamera&&now-lastCameraAt<1500)return;lastCamera=key;lastCameraAt=now;add(raw);},()=>{});if(closed||scanner!==current)await current.stop().catch(()=>{});else camera.textContent='停止扫码 / 스캔 중지';}
   catch(e){if(scanner===current)scanner=null;if(!closed)showError(Error('无法打开相机，可使用扫码枪或输入工牌 / 카메라를 사용할 수 없습니다'));}
   finally{starting=false;}
  };
  render();input.focus();
  return {read(){if(allowEmpty&&!model.people.length)return {workers:[],lead_id:''};model.select(lead.value);return model.read();},async destroy(){closed=true;await stop();}};
 };
})();
