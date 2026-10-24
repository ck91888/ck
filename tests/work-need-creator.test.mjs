import test from 'node:test';import assert from 'node:assert/strict';
import {database} from './d1-adapter.mjs';import {workNeedCreators} from '../worker-v2/work-need-creator.js';
import worker from '../worker-v2/index.js';import {digest} from '../worker-v2/access-control.js';
import {operationHeaders,rememberOperation,confirmOperation} from './office-operator-fixture.mjs';
test('creator uses only this plan creation audit, with honest historical fallback and no last-editor identity',async()=>{
 const DB=database();try{
  const event=(id,revision,actor,action='sop_need_create')=>DB.raw.prepare('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)').run(crypto.randomUUID(),id,revision,action,'QA-ID',actor,'{}','{}','{}','2026-10-15');
  event('SOURCE',0,'原入库客服');event('N',0,'创建客服');DB.raw.prepare('INSERT INTO sop_records VALUES(?,?,?,?,?,?)').run('N','need',4,'bulk','{}','2026-10-15');event('N',4,'最后更新员','sop_need_update');
  const rows=await workNeedCreators({DB},[{id:'N',kind:'need',source_id:'SOURCE',created_by:'管理员'},{id:'OLD',kind:'need',created_by:'历史客服'},{id:'MISSING',kind:'need',source_id:'SOURCE'},{id:'GENERIC',kind:'need',created_by:'管理员'}]);
  assert.deepEqual(rows.map(r=>r.creator_name),['创建客服','历史客服','','']);
 }finally{DB.raw.close();}
});
test('real office login context supplies work-plan creator for list and detail without duplicate user entry',async()=>{
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_WORK_CHAIN_ENABLED:'true',SOP_ACCESS_CONTROL:'true',SOP_ADMIN_CODE_SHA256:await digest('creator-fixture')};let cookie='';
 const call=async(action,data={})=>{const response=await worker.fetch(new Request('https://fixture.test/api',{method:'POST',headers:{'Content-Type':'application/json',Cookie:cookie,...operationHeaders(cookie)},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);if(response.headers.has('set-cookie'))cookie=response.headers.get('set-cookie').split(';')[0];const r=rememberOperation(await response.json(),cookie);assert.equal(r.ok,true,r.error);return r;};
 try{await call('sop_login',{sop_key:'creator-fixture'});await confirmOperation(call,'合成入库客服');const p=await call('v2_inbound_plan_create',{customer:'合成身份客户',biz_classes:['bulk'],planned_box_count:1});await confirmOperation(call,'合成作业客服');
  const n=await call('sop_need_create',{department:'bulk',source_type:'inbound',source_id:p.id,customer:'合成身份客户',title:'身份验证',instructions:'合成作业',created_by:'伪造姓名'});await confirmOperation(call,'合成后续操作员');
  assert.equal((await call('sop_get',{id:n.id})).record.creator_name,'合成作业客服');assert.equal((await call('sop_need_groups',{need_id:n.id})).items[0].items[0].creator_name,'合成作业客服');
 }finally{DB.raw.close();}
});
