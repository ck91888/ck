// Compare the actual published source with the candidate. Supply a separate read-only 6faee8b checkout.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
import {report,DAY} from './workforce-paging-data.mjs';
const {JSDOM}=await import(process.env.CK_JSDOM_MODULE),base=process.env.CK_FE07_BASELINE_ROOT;assert.ok(base,'CK_FE07_BASELINE_ROOT is required');
const cases=[{}, {search:'SYNTHETIC PERSON 001'}, {company:'가온'}, {department:'bulk'}, {status:'working'}, {company:'포레인',department:'direct_ship',status:'rest'}, {search:'TASK-SYN-00001'}, {search:' synthetic person '}, {search:'  '}, {search:'HISTORY-ONLY'}, {search:'EMP-SYN-00099'}],results=[];
async function run(root,role,field){
 const dom=new JSDOM('<!doctype html><main><section id="root"></section></main>',{url:'https://diff.invalid/shuju/',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,host=w.document.querySelector('#root'),data=report(121,{deleted:121}),downloads=[];data.items[103].segments=[{no:'HISTORY-ONLY',department:'import'}];data.items[102].record.name='Synthetic "<&\' 한글';const snapshot=JSON.stringify(data);w.scrollTo=()=>{};
 const interval=w.setInterval.bind(w);w.setInterval=(fn,ms,...args)=>ms===15000?-1:interval(fn,ms,...args);w.CKSession={ready:Promise.resolve(),user:{role},request:async a=>a==='sop_attendance_config'?{day:DAY,asOf:data.asOf,agencies:['가온','포레인'],user:{role}}:a==='sop_attendance_summary'?structuredClone(data):Promise.reject(Error('Unexpected read '+a))};
 w.URL.createObjectURL=b=>{downloads.push(b);return 'blob:synthetic';};w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=function(){};
 try{for(const name of ['document-labels.js','attendance-ui.js'])w.eval(fs.readFileSync(path.join(root,'shared',name),'utf8'));await w.CKLabor(host,{field});const $=s=>host.querySelector('[data-'+s+']'),answers=[];
  function allRows(kind){const rows=[];for(let n=0;n<20;n++){const part=kind==='people'?$('rows'):$('deleted');rows.push(...[...part.querySelectorAll('[data-person]')].map(b=>({id:b.dataset.person,html:b.closest('tr').outerHTML})));const next=$(kind+'-next');if(!next||next.disabled)break;next.click();}return rows;}
  for(const values of cases){for(const [name,value] of [['company',values.company||''],['department-filter',values.department||'all'],['status-filter',values.status||''],['search',values.search||'']]){const input=$(name);input.value=value;input.dispatchEvent(new w.Event(name==='search'?'input':'change'));}
   if($('show-deleted')){$('show-deleted').checked=true;$('show-deleted').dispatchEvent(new w.Event('change'));}
   const normal=allRows('people'),removed=$('deleted')?allRows('deleted'):[],summary=$('summary').innerHTML,agencies=$('agencies').innerHTML;let csv=null;if($('export')){$('export').click();csv=await new Promise(resolve=>{const reader=new w.FileReader();reader.onload=()=>resolve(reader.result);reader.readAsText(downloads.at(-1));});}
   assert.equal(JSON.stringify(data),snapshot);answers.push({normal,removed,summary,agencies,csv});
  }return answers;
 }finally{host.__ckLabor?.destroy();w.close();}
}
for(const [role,field] of [['manager',false],['dispatcher',false],['attendance_reader',false],['dispatcher',true]]){const before=await run(base,role,field),after=await run('.',role,field);assert.deepEqual(after,before);results.push({role,field,cases:cases.length,identical:'all rows across pages incl HTML/actions/order, deleted rows, summaries, companies, full CSV',sha256:createHash('sha256').update(JSON.stringify(after)).digest('hex')});}
const out=process.env.CK_FE07_DIFF_OUT||'/tmp/ck-workforce-paging-diff.json';fs.writeFileSync(out,JSON.stringify({ok:true,synthetic:true,results},null,2));console.log(JSON.stringify({ok:true,combinations:results.length*cases.length,results}));
