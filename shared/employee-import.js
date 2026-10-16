(function(){'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const header=v=>String(v??'').trim().replace(/\s+/g,'').split('/')[0].replace(/[（(].*$/,'');
const actions={create:'新增 / 신규',update:'更新 / 수정',unchanged:'不变 / 유지'};
window.CKEmployeeImport={
 parse(workbook,labels){
  const sheet=workbook.Sheets[workbook.SheetNames[0]];if(!sheet?.['!ref'])throw Error('表格为空 / 빈 파일');
  const range=XLSX.utils.decode_range(sheet['!fullref']||sheet['!ref']);if(range.e.r>2000||range.e.c>63)throw Error('表格过大，请使用模板，每次最多200人 / 최대 200명');
  const matrix=XLSX.utils.sheet_to_json(sheet,{header:1,raw:false,defval:'',blankrows:true});
  const headers=(matrix[0]||[]).map(header),find=names=>headers.findIndex(x=>names.includes(x));
  const cols={name:find(['姓名','이름']),employeeNo:find(['工号','사번']),department:find(['部门','所属部门','부서']),enabled:find(['状态','在职状态','상태'])};
  if(cols.name<0||cols.employeeNo<0||cols.department<0)throw Error('第一行须包含姓名、工号、部门，请下载模板 / 이름·사번·부서 열이 필요합니다');
  for(const names of [['姓名','이름'],['工号','사번'],['部门','所属部门','부서'],['状态','在职状态','상태']])if(headers.filter(x=>names.includes(x)).length>1)throw Error('表头重复，请使用模板 / 중복 열');
  const departments=new Map();for(const [id,label] of Object.entries(labels))for(const text of [id,label,...label.split('/')])departments.set(text.trim().replace(/\s/g,''),id);
  const result=[];
  for(let i=1;i<matrix.length;i++){
   const values=Object.fromEntries(Object.entries(cols).map(([k,col])=>[k,col<0?'':String(matrix[i][col]??'').trim()]));
   if(!Object.values(values).some(Boolean))continue;
   for(const col of Object.values(cols))if(col>=0&&sheet[XLSX.utils.encode_cell({r:i,c:col})]?.f)throw Error('第 '+(i+1)+' 行包含公式，请改为实际文字 / 수식 대신 값을 입력하세요');
   const status=values.enabled.replace(/\s/g,'');
   result.push({...values,row:i+1,department:departments.get(values.department.replace(/\s/g,''))||values.department,enabled:!status?null:['在职','재직','在职/재직'].includes(status)?true:['停用','비활성','停用/비활성'].includes(status)?false:values.enabled});
  }
  if(!result.length)throw Error('没有职员资料，请从第2行开始填写 / 2행부터 입력하세요');
  if(result.length>200)throw Error('每次最多200人，请分批上传 / 최대 200명');return result;
 },
 mount(host,{api,labels,onImported}){
  let rows=null,preview=null,busy=false,done=false,key='';
  host.className='ck-employee-import';host.innerHTML=`<div class="ck-import-heading"><h3>批量登记职员 / 직원 일괄 등록</h3><button type="button" data-close class="ck-small">收起 / 닫기</button></div><p class="ck-muted">填写姓名、工号、部门；同工号更新资料。状态留空：新职员默认在职，已有职员保留原状态。<br>이름·사번·부서 입력. 같은 사번은 정보 갱신. 상태 공란은 기존 상태 유지.</p><div class="ck-import-file"><a class="ck-small" href="/templates/employees.xlsx" download="CK职员登记模板.xlsx">下载模板 / 양식 다운로드</a><label>选择表格 / 파일 선택<input type="file" accept=".xlsx,.xls,.csv" data-file></label><button type="button" data-preview disabled>重新核对 / 다시 확인</button></div><p class="ck-muted">Excel / CSV · 每次最多200人 · 5MB以内。只登记资料，不新增打卡。/ 정보만 등록, 출퇴근 기록 제외.</p><p data-error role="alert" hidden></p><div data-result></div><div class="ck-import-footer"><p data-status role="status">选择文件后显示核对结果 / 파일 선택 후 미리보기</p><button type="button" data-apply disabled>确认导入 / 가져오기</button></div>`;
  const $=name=>host.querySelector('[data-'+name+']'),status=$('status'),error=$('error');
  const fileName=document.createElement('p');fileName.className='ck-muted';fileName.hidden=true;host.querySelector('.ck-import-file').after(fileName);
  function lock(){host.querySelectorAll('button').forEach(b=>b.disabled=busy);$('file').disabled=busy;$('preview').disabled=busy||!rows;$('apply').disabled=busy||done||!preview?.valid;}
  function showError(e){error.hidden=false;error.textContent=e.message;}
  function render(){const c=preview.counts;$('result').innerHTML=`<p class="ck-import-counts">新增 / 신규 <b>${c.create}</b>　更新 / 수정 <b>${c.update}</b>　不变 / 유지 <b>${c.unchanged}</b>　错误 / 오류 <b>${c.error}</b></p><div class="ck-table-wrap ck-import-table"><table class="ck-table"><thead><tr><th>行 / 행</th><th>姓名 / 이름</th><th>工号 / 사번</th><th>部门 / 부서</th><th>状态 / 상태</th><th>核对结果 / 확인 결과</th></tr></thead><tbody>${preview.items.map(p=>{const previous=p.before;const changed=(k,label)=>previous&&previous[k]!==p[k]?`<small>原 / 이전: ${esc(label(previous[k]))}</small>`:'';const dep=d=>labels[d]||d;const enabled=x=>x?'在职 / 재직':'停用 / 비활성';return `<tr class="${p.errors.length?'ck-import-error':''}"><td>${p.row}</td><td>${esc(p.name)}${changed('name',x=>x)}</td><td>${esc(p.employeeNo)}</td><td>${esc(dep(p.department))}${changed('department',dep)}</td><td>${esc(enabled(p.enabled))}${changed('enabled',enabled)}</td><td>${p.errors.length?p.errors.map(esc).join('<br>'):actions[p.action]}</td></tr>`;}).join('')}</tbody></table></div>`;status.textContent=preview.valid?'核对无误后点击确认导入 / 확인 후 가져오기를 누르세요':'请修改错误行后重新上传，当前未保存任何资料 / 오류 수정 후 다시 업로드';}
  async function inspect(){busy=true;preview=null;done=false;key=crypto.randomUUID();error.hidden=true;lock();status.textContent='正在核对… / 확인 중';try{preview=await api('employee_import_preview',{rows});render();}catch(e){showError(e);status.textContent='核对失败，请重试 / 다시 확인하세요';}finally{busy=false;lock();}}
  $('file').onchange=async()=>{const file=$('file').files[0];if(!file)return;fileName.hidden=false;fileName.textContent='当前文件 / 파일: '+file.name;rows=null;preview=null;done=false;$('result').innerHTML='';error.hidden=true;busy=true;lock();try{if(file.size>5*1024*1024||! /\.(xlsx|xls|csv)$/i.test(file.name))throw Error('请选择5MB以内的Excel或CSV文件');rows=CKEmployeeImport.parse(XLSX.read(await file.arrayBuffer(),{type:'array',raw:true,cellNF:true,sheetRows:2002}),labels);}catch(e){showError(e);status.textContent='请修改表格后重新选择文件 / 파일을 수정하세요';}finally{busy=false;lock();$('file').value='';}if(rows)await inspect();};
  $('preview').onclick=inspect;$('close').onclick=()=>{if(!busy)host.hidden=true;};
  $('apply').onclick=async()=>{if(busy||done||!preview?.valid)return;busy=true;error.hidden=true;lock();status.textContent='正在导入… / 가져오는 중';try{const r=await api('employee_import',{rows,previewToken:preview.previewToken,client_req_id:key});done=true;status.textContent=`导入完成：新增 ${r.counts.create}，更新 ${r.counts.update}，不变 ${r.counts.unchanged}。 / 완료: 신규 ${r.counts.create} · 수정 ${r.counts.update} · 유지 ${r.counts.unchanged}`;await onImported();}catch(e){showError(e);status.textContent='导入未确认成功；可重试，或重新核对 / 다시 시도하거나 확인하세요';}finally{busy=false;lock();}};
 }
};
})();
