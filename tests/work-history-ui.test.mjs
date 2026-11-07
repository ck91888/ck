import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
const runtime=process.env.CK_JSDOM_MODULE?await import(process.env.CK_JSDOM_MODULE):null,opts={skip:!runtime};
test('work audit renders business changes, Korea time and safe bilingual fallbacks without raw JSON or credentials',opts,()=>{
 const d=new runtime.JSDOM('<details id="audit"></details>',{runScripts:'outside-only'}),w=d.window;let language='zh';w.getLang=()=>language;w.applyLang=()=>{};
 w.eval(fs.readFileSync(new URL('../shared/work-history-ui.js',import.meta.url),'utf8'));
 const events=[{actor_name:'QA管理员',created_at:'2026-10-02T08:16:34.683Z',action:'sop_work_material_remove',before_json:JSON.stringify({title:'QA打托',status:'pending',password:'QA-NEVER-SHOW',last_material_change:{action:'upload',file_name:'QA-file.csv'}}),after_json:JSON.stringify({title:'QA打托',status:'working',last_material_change:{action:'remove',file_name:'QA-file.csv',api_key:'QA-NEVER-SHOW'}})},
 {actor_name:null,created_at:null,action:'sop_unknown_future_action',before_json:'broken old JSON',after_json:JSON.stringify({status:'unknown-raw-code',deadline:'2026-10-04T15:00:01Z',title:'<img src=x onerror=alert(1)>',token:'QA-NEVER-SHOW'})}];
 const original=JSON.stringify(events),host=w.document.getElementById('audit');w.CKWorkHistory(host,events,{kind:'need'});
 assert.match(host.textContent,/移除作业资料/);assert.match(host.textContent,/17:16:34 KST/);assert.match(host.textContent,/待安排/);assert.match(host.textContent,/作业中/);assert.match(host.textContent,/2026-10-05/);assert.match(host.textContent,/时间未记录/);assert.match(host.textContent,/未识别状态/);
 for(const secret of ['QA-NEVER-SHOW','password','api_key','unknown-raw-code','sop_unknown_future_action','sop_work_material_remove','T08:16'])assert.ok(!host.textContent.includes(secret),secret);assert.equal(host.querySelector('img'),null);assert.equal(JSON.stringify(events),original);
 host.open=true;host.querySelector('.ck-audit-event details').open=true;language='ko';w.applyLang();assert.match(host.textContent,/작업 자료 제거/);assert.match(host.textContent,/작업 중/);assert.equal(host.open,true);assert.equal(host.querySelector('.ck-audit-event details').open,true);w.close();
});
