import test from 'node:test';
import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';
import {database} from './d1-adapter.mjs';

const sourceLines = [{unit_type:'carton',actual_qty:12},{unit_type:'pallet',actual_qty:2}];
const png = () => new File([Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aB9sAAAAASUVORK5CYII=','base64')],'fixture-arrival.png',{type:'image/png'});

async function setup(t) {
  const DB=database(), objects=new Map();
  t.after(()=>DB.raw.close());
  const env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',
    SOP_USERS_JSON:JSON.stringify([
      {id:'M',name:'合成管理员',role:'manager',scope:'office',key:'fixture'},
      {id:'S',name:'合成客服',role:'service',scope:'office',departments:['bulk','direct_ship'],key:'service'},
      {id:'R',name:'合成审核员',role:'reviewer',scope:'office',departments:['bulk','direct_ship'],key:'reviewer'},
      {id:'V',name:'合成只读用户',role:'viewer',scope:'office',key:'viewer'},
      {id:'F',name:'合成现场用户',role:'manager',scope:'field',key:'field'},
    ]),
    R2_BUCKET:{async put(key,body,meta){objects.set(key,{body,httpMetadata:meta.httpMetadata});},async get(key){return objects.get(key);},async delete(key){objects.delete(key);}},
  };
  let cookie='';
  async function raw(body,options={}) {
    const multi=body instanceof FormData, method=options.method||'POST';
    const response=await worker.fetch(new Request('https://fixture.local/api'+(method==='GET'?'?'+new URLSearchParams(body):''),{
      method,headers:{Cookie:cookie,...(multi?{}:{'Content-Type':'application/json'}),...options.headers},
      ...(method==='GET'?{}:{body:multi?body:JSON.stringify(body)}),
    }),env);
    if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];
    return response.json();
  }
  const call=(action,body={},options)=>raw({action,client_req_id:crypto.randomUUID(),...body},options);
  const ok=async(action,body={},options)=>{const result=await call(action,body,options);assert.equal(result.ok,true,JSON.stringify(result));return result;};
  const login=(key='fixture')=>ok('sop_login',{sop_key:key});
  await login();
  const upload=async(jobId,feedbackId)=>{
    const form=new FormData();
    for(const [key,value] of Object.entries({action:'v2_attachment_upload',attachment_category:'unload_photo',job_id:jobId,related_doc_type:'field_feedback',related_doc_id:feedbackId,client_req_id:crypto.randomUUID()}))form.set(key,value);
    form.set('file',png());const result=await raw(form);assert.equal(result.ok,true,JSON.stringify(result));return result;
  };
  const source=async(lines=sourceLines,{finish=true,photo=false}={})=>{
    const started=await ok('sop_native_start',{
      payload:{action:'v2_unplanned_unload_start',cargo_summary:'合成计划外货物',customer:'不应自动采信的内容',remark:'保留现场合成卸货备注',client_req_id:crypto.randomUUID()},
      workers:[{id:'A',name:'合成员工甲'},{id:'B',name:'合成员工乙'},{id:'C',name:'合成员工丙'}],lead_id:'A',estimated_minutes:15,
    });
    const feedback_id=DB.raw.prepare('SELECT related_doc_id FROM v2_ops_jobs WHERE id=?').get(started.job_id).related_doc_id;
    DB.raw.prepare('UPDATE v2_ops_job_workers SET joined_at=? WHERE job_id=?').run(new Date(Date.now()-20*60000).toISOString(),started.job_id);
    const attachment=photo?await upload(started.job_id,feedback_id):null;
    if(finish)await ok('v2_unplanned_unload_finish',{job_id:started.job_id,worker_id:'A',complete_job:true,result_lines:lines,remark:'保留现场合成卸货备注'});
    return {job_id:started.job_id,feedback_id,attachment};
  };
  const preview=f=>ok('sop_feedback_link_create_preview',{feedback_id:f.feedback_id});
  const genericSource=async()=>{
    const result=await ok('v2_feedback_create',{feedback_type:'unload_no_doc',title:'合成历史无单反馈',content:'无实际卸货任务的合成历史记录',submitted_by:'合成现场员工'});
    return {feedback_id:result.id};
  };
  const genericBody=(f,extra={})=>({feedback_id:f.feedback_id,customer:'明确填写的合成历史客户',biz_class:'bulk',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:18,actual_qty:999},{unit_type:'pallet',planned_qty:3,actual_qty:999}],...extra});
  const body=(p,extra={})=>({
    customer:'明确填写的合成客户',biz_class:'bulk',biz_classes:['bulk'],cargo_summary:'办公室合成计划货物',remark:'办公室合成备注',
    lines:p.source.actual_totals.map(({unit_type,actual_qty})=>({unit_type,planned_qty:actual_qty})),
    ...extra,feedback_context:{feedback_id:p.source.feedback_id,feedback_version:p.source.feedback_version,job_version:p.source.job_version},
  });
  const create=(p,extra={})=>ok('v2_inbound_plan_create',body(p,extra));
  const all=table=>DB.raw.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
  const businessSnapshot=()=>Object.fromEntries(DB.raw.prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'v2_%' OR name IN ('ck_feedback_plan_links','ck_unload_plan_links','sop_records','sop_events')) AND name!='v2_idempotency_keys' ORDER BY name").all().map(({name})=>[name,all(name)]));
  const originalWork=()=>Object.fromEntries(['v2_ops_job_workers','v2_ops_job_results','v2_attachments'].map(table=>[table,all(table)]));
  const counts=()=>Object.fromEntries(['v2_inbound_plans','v2_inbound_plan_lines','v2_inbound_plan_biz_tasks','ck_feedback_plan_links','ck_unload_plan_links'].map(table=>[table,DB.raw.prepare(`SELECT count(*) n FROM ${table}`).get().n]));
  return {DB,env,objects,raw,call,ok,login,source,preview,genericSource,genericBody,body,create,all,businessSnapshot,originalWork,counts};
}

test('feedback create preview reads completed source quantities and does not mutate business records',async t=>{
  const s=await setup(t),f=await s.source(),before=s.businessSnapshot(),p=await s.preview(f);
  assert.equal(p.existing_link,null);
  assert.equal(p.source.feedback_id,f.feedback_id);assert.equal(p.source.job_id,f.job_id);
  assert.ok(p.source.display_no);assert.ok(p.source.feedback_version);assert.ok(p.source.job_version);assert.ok(p.source.completed_at);
  assert.match(p.source.content,/不应自动采信/);assert.match(p.source.remark,/现场合成卸货备注/);
  assert.deepEqual(p.source.actual_totals,sourceLines);assert.equal(p.source.workers.length,3);
  assert.deepEqual(s.businessSnapshot(),before);
  const repeat=await s.preview(f);assert.deepEqual(repeat,p);assert.deepEqual(s.businessSnapshot(),before);
});

for(const role of ['fixture','service','reviewer'])test(`office ${role} can preview and create from completed feedback`,async t=>{
  const s=await setup(t),f=await s.source();await s.login(role);
  const p=await s.preview(f),out=await s.create(p);
  assert.equal(out.id,out.inbound_plan_id);assert.equal(out.job_id,f.job_id);assert.ok(out.display_no);
});

for(const role of ['field','viewer'])test(`${role} cannot preview or create feedback attribution`,async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),before=s.businessSnapshot();await s.login(role);
  assert.equal((await s.call('sop_feedback_link_create_preview',{feedback_id:f.feedback_id})).ok,false);
  assert.equal((await s.call('v2_inbound_plan_create',s.body(p))).ok,false);
  assert.deepEqual(s.businessSnapshot(),before);
});

test('standard create atomically attributes the original job and preserves labor, output, and arrival photo',async t=>{
  const s=await setup(t),f=await s.source(sourceLines,{photo:true}),p=await s.preview(f);
  const work=s.originalWork(),job=s.all('v2_ops_jobs')[0],objects=[...s.objects.keys()],batches=[],original=s.DB.batch;
  s.DB.batch=async statements=>{batches.push(statements.map(x=>x.sql));return original(statements);};
  const out=await s.create(p);s.DB.batch=original;
  assert.equal(out.id,out.inbound_plan_id);assert.equal(out.job_id,f.job_id);assert.ok(out.display_no);
  const creation=batches.filter(statements=>statements.some(sql=>/INSERT\s+INTO v2_inbound_plans\b/i.test(sql)));
  assert.equal(creation.length,1);
  for(const pattern of [/INSERT INTO ck_feedback_plan_links/i,/INSERT INTO ck_unload_plan_links/i,/UPDATE v2_ops_jobs/i,/UPDATE v2_field_feedbacks/i,/INSERT INTO v2_inbound_plan_lines/i])assert.ok(creation[0].some(sql=>pattern.test(sql)),String(pattern));
  assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('v2_ops_jobs').length,1);
  assert.deepEqual(s.originalWork(),work);assert.deepEqual([...s.objects.keys()],objects);
  assert.deepEqual({...s.all('v2_ops_jobs')[0]},{...job,related_doc_type:'inbound_plan',related_doc_id:out.id});
  const link=s.all('ck_feedback_plan_links')[0];assert.equal(link.feedback_id,f.feedback_id);assert.equal(link.plan_id,out.id);assert.equal(link.job_id,f.job_id);
  const unloadLink=s.all('ck_unload_plan_links')[0];assert.equal(unloadLink.job_id,f.job_id);assert.equal(unloadLink.plan_id,out.id);
  const d=await s.ok('v2_inbound_plan_detail',{id:out.id});
  assert.equal(d.plan.customer,'明确填写的合成客户');assert.equal(d.plan.status,'completed');
  assert.equal(d.unload_summary.completed,true);assert.equal(d.unload_summary.total_minutes,60);assert.equal(d.jobs[0].id,f.job_id);
  assert.deepEqual(d.lines.map(l=>({unit_type:l.unit_type,planned_qty:l.planned_qty,actual_qty:l.actual_qty})),sourceLines.map(l=>({...l,planned_qty:l.actual_qty})));
  assert.equal(d.arrival_photos[0].id,f.attachment.id);assert.equal(d.existing_feedback_link.feedback_id,f.feedback_id);
  const fd=await s.ok('v2_feedback_detail',{id:f.feedback_id});assert.equal(fd.feedback.status,'converted');assert.equal(fd.feedback.inbound_plan_id,out.id);
  assert.equal(fd.existing_plan_link.plan_id,out.id);assert.equal(fd.arrival_photos[0].id,f.attachment.id);
});

for(const [classes,status] of [
  [['bulk'],'completed'],[['return'],'completed'],[['change_order'],'completed'],
  [['direct_ship'],'arrived_pending_putaway'],[['bulk_putaway'],'arrived_pending_putaway'],[['bulk','direct_ship'],'partially_completed'],
])test(`feedback create preserves ${classes.join('+')} completion rules`,async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f);
  const out=await s.create(p,{biz_class:classes[0],biz_classes:classes,external_inbound_nos:['FIXTURE-EXT-1','FIXTURE-EXT-2'],bulk_external_inbound_nos:['FIXTURE-BULK-1']});
  const d=await s.ok('v2_inbound_plan_detail',{id:out.id});assert.equal(d.plan.status,status);
  assert.ok(d.plan.unload_completed_at);assert.equal(d.unload_summary.completed,true);
  if(classes.some(x=>['direct_ship','bulk_putaway'].includes(x)))assert.ok(d.lines.every(l=>Number(l.putaway_qty||0)===0));
  assert.deepEqual(s.all('v2_ops_job_results').map(r=>r.job_id),[f.job_id]);
});

test('planned edits do not forge source quantities; missing pallet unit receives its own zero-plan actual line',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),work=s.originalWork();
  const out=await s.create(p,{lines:[{unit_type:'carton',planned_qty:25,actual_qty:999}],actual_qty:111,result_lines:[{unit_type:'pallet',actual_qty:999}]});
  const d=await s.ok('v2_inbound_plan_detail',{id:out.id});
  assert.deepEqual(d.lines.map(l=>({unit_type:l.unit_type,planned_qty:l.planned_qty,actual_qty:l.actual_qty})),[
    {unit_type:'carton',planned_qty:25,actual_qty:12},{unit_type:'pallet',planned_qty:0,actual_qty:2},
  ]);
  assert.deepEqual(s.originalWork(),work);
  const stored=JSON.parse(s.all('ck_unload_plan_links')[0].result_json);
  assert.deepEqual(stored.result_lines.map(({unit_type,actual_qty})=>({unit_type,actual_qty})),sourceLines);
});

test('duplicate normalized source-unit lines are rejected without guessing the distribution',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),before=s.businessSnapshot();
  for(const duplicate of ['carton','box']){
    const r=await s.call('v2_inbound_plan_create',s.body(p,{lines:[{unit_type:'carton',planned_qty:6},{unit_type:duplicate,planned_qty:6}]}));
    assert.equal(r.ok,false,JSON.stringify(r));assert.deepEqual(s.businessSnapshot(),before);
  }
});

test('customer must be explicitly supplied instead of inferred from feedback text',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),before=s.businessSnapshot();
  for(const customer of ['', '   '])assert.equal((await s.call('v2_inbound_plan_create',s.body(p,{customer}))).ok,false);
  assert.deepEqual(s.businessSnapshot(),before);
});

test('preview uses feedback output first, then latest result, then shared job output, keeping pallet separate',async t=>{
  const s=await setup(t),f=await s.source();
  s.DB.raw.prepare('UPDATE v2_ops_job_results SET result_lines_json=? WHERE job_id=?').run(JSON.stringify([{unit_type:'pallet',actual_qty:7}]),f.job_id);
  s.DB.raw.prepare('UPDATE v2_ops_jobs SET shared_result_json=? WHERE id=?').run(JSON.stringify({result_lines:[{unit_type:'carton',actual_qty:4}]}),f.job_id);
  assert.deepEqual((await s.preview(f)).source.actual_totals,sourceLines);
  s.DB.raw.prepare("UPDATE v2_field_feedbacks SET result_lines_json='[]' WHERE id=?").run(f.feedback_id);
  assert.deepEqual((await s.preview(f)).source.actual_totals,[{unit_type:'pallet',actual_qty:7}]);
  s.DB.raw.prepare("UPDATE v2_ops_job_results SET result_lines_json='[]' WHERE job_id=?").run(f.job_id);
  assert.deepEqual((await s.preview(f)).source.actual_totals,[{unit_type:'carton',actual_qty:4}]);
});

for(const entity of ['feedback','job'])test(`stale ${entity} version refuses creation and preserves the source`,async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f);
  const table=entity==='feedback'?'v2_field_feedbacks':'v2_ops_jobs',id=entity==='feedback'?f.feedback_id:f.job_id;
  s.DB.raw.prepare(`UPDATE ${table} SET updated_at=? WHERE id=?`).run('fixture-new-version',id);
  const before=s.businessSnapshot();assert.equal((await s.call('v2_inbound_plan_create',s.body(p))).ok,false);
  assert.deepEqual(s.businessSnapshot(),before);
});

test('unfinished unload or completed unload with a live worker cannot preview or create',async t=>{
  const s=await setup(t),active=await s.source(sourceLines,{finish:false});
  assert.equal((await s.call('sop_feedback_link_create_preview',{feedback_id:active.feedback_id})).ok,false);
  await s.ok('v2_unplanned_unload_finish',{job_id:active.job_id,worker_id:'A',complete_job:true,result_lines:sourceLines});
  const f=active,p=await s.preview(f);s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=NULL WHERE job_id=? AND worker_id='A'").run(f.job_id);
  const before=s.businessSnapshot();
  assert.equal((await s.call('sop_feedback_link_create_preview',{feedback_id:f.feedback_id})).ok,false);
  assert.equal((await s.call('v2_inbound_plan_create',s.body(p))).ok,false);assert.deepEqual(s.businessSnapshot(),before);
});

for(const change of ['version','live-worker'])test(`commit guard rejects a ${change} change after create preflight`,async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),counts=s.counts(),work=s.originalWork(),original=s.DB.batch;let injected=false;
  s.DB.batch=async statements=>{
    if(!injected&&statements.some(x=>/INSERT\s+INTO v2_inbound_plans\b/i.test(x.sql))){
      injected=true;
      if(change==='version')s.DB.raw.prepare("UPDATE v2_field_feedbacks SET updated_at='fixture-concurrent-edit' WHERE id=?").run(f.feedback_id);
      else s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=NULL WHERE job_id=? AND worker_id='A'").run(f.job_id);
    }
    return original(statements);
  };
  const r=await s.call('v2_inbound_plan_create',s.body(p));s.DB.batch=original;
  assert.equal(injected,true);assert.equal(r.ok,false,JSON.stringify(r));assert.deepEqual(s.counts(),counts);
  assert.equal(s.all('v2_ops_jobs')[0].related_doc_type,'field_feedback');assert.equal(s.all('v2_field_feedbacks')[0].status,'unloaded_pending_info');
  assert.deepEqual(s.all('v2_ops_job_results'),work.v2_ops_job_results);
});

test('lost response and fresh-request retries return the one canonical plan; linked preview stays read-only',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),request={...s.body(p),client_req_id:crypto.randomUUID()};
  const first=await s.ok('v2_inbound_plan_create',request);
  const retry=await s.ok('v2_inbound_plan_create',request),fresh=await s.ok('v2_inbound_plan_create',{...request,client_req_id:crypto.randomUUID()});
  for(const r of [retry,fresh]){assert.equal(r.id,first.id);assert.equal(r.inbound_plan_id,first.id);assert.equal(r.job_id,f.job_id);}
  assert.equal(fresh.already_linked,true);assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('ck_feedback_plan_links').length,1);
  const before=s.businessSnapshot(),linked=await s.preview(f);assert.equal(linked.existing_link.plan_id,first.id);assert.equal(linked.existing_link.display_no,first.display_no);assert.deepEqual(s.businessSnapshot(),before);
});

test('two simultaneous create requests resolve to one plan and one canonical unload attribution',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),work=s.originalWork();
  const results=await Promise.all([s.call('v2_inbound_plan_create',s.body(p)),s.call('v2_inbound_plan_create',s.body(p))]);
  for(const r of results)assert.equal(r.ok,true,JSON.stringify(r));
  assert.equal(results[0].id,results[1].id);assert.equal(s.all('v2_inbound_plans').length,1);
  assert.equal(s.all('ck_feedback_plan_links').length,1);assert.equal(s.all('ck_unload_plan_links').length,1);assert.deepEqual(s.originalWork(),work);
});

test('a second request committed after source preflight wins even when the first allocates a different plan number',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),prepare=s.DB.prepare;let injected=false,inner;
  s.DB.prepare=sql=>{
    const statement=prepare(sql);
    if(!injected&&sql.includes('SELECT display_no FROM v2_inbound_plans WHERE plan_date=')){
      const first=statement.first.bind(statement);
      statement.first=async()=>{injected=true;s.DB.prepare=prepare;inner=await s.ok('v2_inbound_plan_create',s.body(p));return first();};
    }
    return statement;
  };
  const outer=await s.call('v2_inbound_plan_create',s.body(p));s.DB.prepare=prepare;
  assert.equal(injected,true);assert.equal(outer.ok,true,JSON.stringify(outer));assert.equal(outer.id,inner.id);assert.equal(outer.already_linked,true);
  assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('ck_feedback_plan_links').length,1);assert.equal(s.all('ck_unload_plan_links').length,1);
});

test('failure at feedback conversion rolls back the whole graph and retry preserves the original source work',async t=>{
  const s=await setup(t),f=await s.source(sourceLines,{photo:true}),p=await s.preview(f),before=s.businessSnapshot(),request={...s.body(p),client_req_id:crypto.randomUUID()};
  s.DB.raw.exec("CREATE TRIGGER fixture_feedback_create_fault BEFORE UPDATE ON v2_field_feedbacks WHEN NEW.status='converted' BEGIN SELECT RAISE(ABORT,'fixture converted write fault'); END");
  const result=await s.call('v2_inbound_plan_create',request);assert.equal(result.ok,false,JSON.stringify(result));
  assert.deepEqual(s.businessSnapshot(),before);assert.equal(s.objects.size,1);
  s.DB.raw.exec('DROP TRIGGER fixture_feedback_create_fault');
  const retry=await s.ok('v2_inbound_plan_create',request);assert.equal(retry.job_id,f.job_id);assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('ck_feedback_plan_links').length,1);
  for(const table of ['v2_ops_job_workers','v2_ops_job_results','v2_attachments'])assert.deepEqual(s.all(table),before[table]);
});

test('same-request retry repairs postcommit completion failure using the cached original create result',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),work=s.originalWork(),request={...s.body(p),client_req_id:crypto.randomUUID()};
  s.DB.raw.exec("CREATE TRIGGER fixture_feedback_reconcile_fault BEFORE UPDATE ON v2_inbound_plan_biz_tasks WHEN NEW.completion_source='unload' BEGIN SELECT RAISE(ABORT,'fixture postcommit completion fault'); END");
  const first=await s.call('v2_inbound_plan_create',request);assert.equal(first.ok,false,JSON.stringify(first));
  assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('v2_ops_jobs').length,1);
  assert.equal(s.all('ck_feedback_plan_links').length,1);assert.equal(s.all('ck_unload_plan_links').length,1);
  const id=s.all('v2_inbound_plans')[0].id,cached=s.DB.raw.prepare('SELECT response_json FROM v2_idempotency_keys WHERE idem_key=?').get(request.client_req_id);
  assert.equal(JSON.parse(cached.response_json).id,id);assert.equal(s.all('v2_field_feedbacks')[0].status,'converted');assert.deepEqual(s.originalWork(),work);
  s.DB.raw.exec('DROP TRIGGER fixture_feedback_reconcile_fault');
  const retry=await s.ok('v2_inbound_plan_create',request);assert.equal(retry.id,id);assert.equal(retry.inbound_plan_id,id);assert.equal(retry.already_linked,undefined);assert.equal(retry.status,'completed');
  assert.equal(s.all('v2_inbound_plans')[0].status,'completed');assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('v2_ops_jobs').length,1);
  assert.equal(s.all('ck_feedback_plan_links').length,1);assert.equal(s.all('ck_unload_plan_links').length,1);assert.deepEqual(s.originalWork(),work);
});

test('legacy finalize with a new request refuses feedback already converted through standard create',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),out=await s.create(p),before=s.businessSnapshot();
  const replay=await s.call('v2_feedback_finalize_to_inbound',{feedback_id:f.feedback_id,customer:'不得产生第二计划',biz_class:'bulk',lines:[{unit_type:'carton',actual_qty:999}]});
  assert.equal(replay.ok,false);assert.equal(s.all('v2_inbound_plans')[0].id,out.id);assert.deepEqual(s.businessSnapshot(),before);
});

test('feedback already attributed to an existing plan resolves to that plan without making another',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f);
  const existing=await s.ok('v2_inbound_plan_create',{customer:'合成既有计划',biz_class:'bulk',biz_classes:['bulk'],lines:[{unit_type:'carton',planned_qty:12},{unit_type:'pallet',planned_qty:2}]});
  const linkPreview=await s.ok('sop_feedback_link_preview',{feedback_id:f.feedback_id,plan_id:existing.id});
  await s.ok('sop_feedback_link_save',{feedback_id:f.feedback_id,plan_id:existing.id,feedback_version:linkPreview.feedback_version,job_version:linkPreview.job_version,plan_version:linkPreview.plan_version,lines:linkPreview.lines.map(l=>({line_id:l.id,unit_type:l.unit_type,actual_qty:l.actual_qty}))});
  const before=s.businessSnapshot(),out=await s.create(p);
  assert.equal(out.id,existing.id);assert.equal(out.inbound_plan_id,existing.id);assert.equal(out.already_linked,true);assert.deepEqual(s.businessSnapshot(),before);
  assert.equal((await s.preview(f)).existing_link.plan_id,existing.id);
});

test('request key reuse for a different feedback cannot silently return or consume the first source',async t=>{
  const s=await setup(t),a=await s.source(),pa=await s.preview(a),requestId=crypto.randomUUID();
  await s.create(pa,{client_req_id:requestId});
  const b=await s.source(),pb=await s.preview(b),before=s.businessSnapshot();
  const result=await s.call('v2_inbound_plan_create',s.body(pb,{client_req_id:requestId}));
  assert.equal(result.ok,false,JSON.stringify(result));assert.deepEqual(s.businessSnapshot(),before);
  assert.equal(s.DB.raw.prepare('SELECT status FROM v2_field_feedbacks WHERE id=?').get(b.feedback_id).status,'unloaded_pending_info');
});

test('fresh legacy finalize uses the original completed job and source quantities through canonical attribution',async t=>{
  const s=await setup(t),f=await s.source(sourceLines,{photo:true}),work=s.originalWork();
  const request={feedback_id:f.feedback_id,client_req_id:crypto.randomUUID(),customer:'合成旧入口明确客户',biz_class:'bulk',lines:[{unit_type:'carton',planned_qty:20,actual_qty:999}]};
  const out=await s.ok('v2_feedback_finalize_to_inbound',request);
  const id=out.id||out.inbound_plan_id,detail=await s.ok('v2_inbound_plan_detail',{id});
  assert.equal(out.job_id,f.job_id);assert.equal(detail.plan.status,'completed');assert.equal(s.all('v2_ops_jobs').length,1);
  assert.deepEqual(detail.lines.map(l=>({unit_type:l.unit_type,actual_qty:l.actual_qty})),sourceLines);
  assert.equal(s.all('ck_feedback_plan_links')[0].plan_id,id);assert.equal(s.all('ck_unload_plan_links')[0].job_id,f.job_id);assert.deepEqual(s.originalWork(),work);
  const before=s.businessSnapshot(),replay=await s.ok('v2_feedback_finalize_to_inbound',request);
  assert.equal(replay.id||replay.inbound_plan_id,id);assert.deepEqual(s.businessSnapshot(),before);
});

for(const invalid of ['missing-job','active-crew'])test(`legacy finalize rejects ${invalid} feedback without any new plan`,async t=>{
  const s=await setup(t),f=await s.source();
  if(invalid==='missing-job')s.DB.raw.prepare("UPDATE v2_field_feedbacks SET related_doc_id='fixture-missing-job' WHERE id=?").run(f.feedback_id);
  else s.DB.raw.prepare("UPDATE v2_ops_job_workers SET left_at=NULL WHERE job_id=? AND worker_id='A'").run(f.job_id);
  const before=s.businessSnapshot(),r=await s.call('v2_feedback_finalize_to_inbound',{feedback_id:f.feedback_id,customer:'合成旧入口客户',biz_class:'bulk',lines:[{unit_type:'carton',actual_qty:999}]});
  assert.equal(r.ok,false,JSON.stringify(r));assert.deepEqual(s.businessSnapshot(),before);
});

test('legacy generic open feedback without a job creates a pending plan with zero actual quantities',async t=>{
  const s=await setup(t),f=await s.genericSource(),out=await s.ok('v2_feedback_convert_to_inbound',s.genericBody(f));
  assert.equal(out.id,out.inbound_plan_id);assert.ok(out.display_no);
  const plan=s.all('v2_inbound_plans')[0];assert.equal(plan.id,out.id);assert.equal(plan.status,'pending');assert.equal(plan.source_feedback_id,f.feedback_id);assert.equal(plan.unload_completed_at||'','');
  assert.deepEqual(s.all('v2_inbound_plan_lines').map(l=>({unit_type:l.unit_type,planned_qty:l.planned_qty,actual_qty:Number(l.actual_qty||0)})),[
    {unit_type:'carton',planned_qty:18,actual_qty:0},{unit_type:'pallet',planned_qty:3,actual_qty:0},
  ]);
  assert.equal(s.all('v2_ops_jobs').length,0);assert.equal(s.all('v2_ops_job_workers').length,0);assert.equal(s.all('v2_ops_job_results').length,0);
  assert.equal(s.all('ck_feedback_plan_links').length,0);assert.equal(s.all('ck_unload_plan_links').length,0);
  assert.equal(s.all('v2_field_feedbacks')[0].status,'converted');assert.equal(s.all('v2_field_feedbacks')[0].inbound_plan_id,out.id);
  assert.ok(s.all('v2_inbound_plan_biz_tasks').every(task=>task.status==='pending'));
});

test('two generic conversions of the same no-job source create only one pending plan',async t=>{
  const s=await setup(t),f=await s.genericSource();
  const results=await Promise.all([s.call('v2_feedback_convert_to_inbound',s.genericBody(f)),s.call('v2_feedback_convert_to_inbound',s.genericBody(f))]);
  const successes=results.filter(r=>r.ok);assert.ok(successes.length>=1,JSON.stringify(results));
  assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('v2_inbound_plan_lines').length,2);assert.equal(s.all('v2_inbound_plan_biz_tasks').length,1);
  const id=s.all('v2_inbound_plans')[0].id;for(const r of successes)assert.equal(r.id||r.inbound_plan_id,id);
  assert.equal(s.all('v2_field_feedbacks')[0].inbound_plan_id,id);assert.equal(s.all('v2_ops_jobs').length,0);
});

test('generic no-job conversion rolls its entire batch back when feedback conversion fails',async t=>{
  const s=await setup(t),f=await s.genericSource(),before=s.businessSnapshot(),request=s.genericBody(f,{client_req_id:crypto.randomUUID()});
  s.DB.raw.exec("CREATE TRIGGER fixture_generic_feedback_fault BEFORE UPDATE ON v2_field_feedbacks WHEN NEW.status='converted' BEGIN SELECT RAISE(ABORT,'fixture generic feedback fault'); END");
  assert.equal((await s.call('v2_feedback_convert_to_inbound',request)).ok,false);assert.deepEqual(s.businessSnapshot(),before);
  assert.equal(s.DB.raw.prepare('SELECT count(*) n FROM v2_idempotency_keys WHERE idem_key=?').get(request.client_req_id).n,0);
  s.DB.raw.exec('DROP TRIGGER fixture_generic_feedback_fault');
  const retry=await s.ok('v2_feedback_convert_to_inbound',request);assert.equal(s.all('v2_inbound_plans')[0].id,retry.id);assert.equal(s.all('v2_ops_jobs').length,0);
});

for(const role of ['field','viewer'])test(`${role} cannot convert legacy generic no-job feedback`,async t=>{
  const s=await setup(t),f=await s.genericSource(),before=s.businessSnapshot();await s.login(role);
  assert.equal((await s.call('v2_feedback_convert_to_inbound',s.genericBody(f))).ok,false);assert.deepEqual(s.businessSnapshot(),before);
});

test('generic unload_no_doc with a real completed job uses canonical attribution and source quantities',async t=>{
  const s=await setup(t),f=await s.source(sourceLines,{photo:true}),work=s.originalWork();
  s.DB.raw.prepare("UPDATE v2_field_feedbacks SET feedback_type='unload_no_doc' WHERE id=?").run(f.feedback_id);
  const out=await s.ok('v2_feedback_convert_to_inbound',s.genericBody(f)),id=out.id||out.inbound_plan_id;
  assert.equal(out.job_id,f.job_id);assert.equal(s.all('v2_inbound_plans')[0].status,'completed');
  assert.deepEqual(s.all('v2_inbound_plan_lines').map(l=>({unit_type:l.unit_type,actual_qty:l.actual_qty})),sourceLines);
  assert.equal(s.all('ck_feedback_plan_links')[0].feedback_id,f.feedback_id);assert.equal(s.all('ck_feedback_plan_links')[0].plan_id,id);
  assert.equal(s.all('ck_unload_plan_links')[0].job_id,f.job_id);assert.equal(s.all('v2_ops_jobs')[0].related_doc_id,id);assert.equal(s.all('v2_ops_jobs').length,1);assert.deepEqual(s.originalWork(),work);
});

test('concurrent reuse of one request key for different feedback rejects the loser without consuming its source',async t=>{
  const s=await setup(t),a=await s.source(),pa=await s.preview(a),b=await s.source(),pb=await s.preview(b),requestId=crypto.randomUUID();
  const sourceBefore=s.DB.raw.prepare('SELECT * FROM v2_field_feedbacks WHERE id=?').get(b.feedback_id),jobBefore=s.DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(b.job_id),work=s.originalWork();
  const original=s.DB.batch;let injected=false,winner;
  s.DB.batch=async statements=>{
    if(!injected&&statements.some(statement=>/INSERT\s+INTO v2_inbound_plans\b/i.test(statement.sql))){
      injected=true;winner=await s.create(pa,{client_req_id:requestId});
    }
    return original(statements);
  };
  const loser=await s.call('v2_inbound_plan_create',s.body(pb,{client_req_id:requestId}));s.DB.batch=original;
  assert.equal(injected,true);assert.equal(loser.ok,false,JSON.stringify(loser));assert.equal(s.all('v2_inbound_plans').length,1);assert.equal(s.all('v2_inbound_plans')[0].id,winner.id);
  assert.deepEqual(s.DB.raw.prepare('SELECT * FROM v2_field_feedbacks WHERE id=?').get(b.feedback_id),sourceBefore);
  assert.deepEqual(s.DB.raw.prepare('SELECT * FROM v2_ops_jobs WHERE id=?').get(b.job_id),jobBefore);assert.deepEqual(s.originalWork(),work);
  assert.equal(s.all('ck_feedback_plan_links').length,1);assert.equal(s.all('ck_feedback_plan_links')[0].feedback_id,a.feedback_id);assert.equal(s.all('ck_unload_plan_links').length,1);
});

test('an existing linked plan ID cannot be used as a feedback ID in preview or standard create',async t=>{
  const s=await setup(t),f=await s.source(),p=await s.preview(f),out=await s.create(p),before=s.businessSnapshot();
  assert.equal((await s.preview(f)).existing_link.plan_id,out.id);
  assert.equal((await s.call('sop_feedback_link_create_preview',{feedback_id:out.id})).ok,false);
  const request=s.body(p);request.feedback_context.feedback_id=out.id;
  assert.equal((await s.call('v2_inbound_plan_create',request)).ok,false);assert.deepEqual(s.businessSnapshot(),before);
});
