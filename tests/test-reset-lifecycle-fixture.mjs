// Local synthetic fixture only; the staging database ID is an authorization
// marker in env, never a remote database connection or Cloudflare API call.
import assert from 'node:assert/strict';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {localD1} from './d1-local-runtime.mjs';
import entry from '../worker-v2/staging-entry.js';
import {STAGING_DATABASE,STAGING_HOST} from '../worker-v2/test-data-reset.js';
import {operationHeaders} from './office-operator-fixture.mjs';
export async function resetLifecycleFixture(t){
 const day=new Date(Date.now()+9*3600000).toISOString().slice(0,10);t.mock.timers.enable({apis:['Date'],now:Date.parse(day+'T09:00:00+09:00')});
 const f=await ownedHomeFixture(),objects=new Map();let native;t.after(async()=>{await native?.close();f.DB.raw.close();});
 f.env.R2_BUCKET={async put(key,body,metadata){objects.set(key,{body,metadata});},async get(key){return objects.get(key);},async delete(){throw Error('Synthetic reset must retain R2 objects');}};
 Object.assign(f.env,{SOP_ATTENDANCE_RETURNS_ENABLED:'true',SOP_AUTO_DISPATCH_TIME_ENABLED:'true',SOP_DISPATCH_SHARE_ENABLED:'true',SOP_DAILY_CUTOFF_ENABLED:'true',SOP_ATTENDANCE_SHIFT_ACCOUNTING:'true',SOP_TEST_RESET_ENABLED:'true',SOP_TEST_RESET_DATABASE:STAGING_DATABASE});
 const preview=()=>f.ok('office','sop_attendance_return_preview',{badge:f.people.A.badgeId});
 const cursor=p=>({badge:f.people.A.badgeId,id:p.record.id,version:p.version,span_id:p.span_id,span_version:p.span_version});
 const first=await f.task('A','合成重置前派审');t.mock.timers.tick(10*60000);
 await f.ok('office','sop_attendance_checkout',cursor(await preview()));t.mock.timers.tick(10*60000);
 await f.ok('office','sop_attendance_return_confirm',{...cursor(await preview()),confirmed:true});await f.ok('A','sop_login',{badge:f.people.A.badgeId});
 const second=await f.task('A','合成返岗后新派审'),pair=await f.ok('A','sop_dispatch_share_invite',{target_badge:f.people.B.badgeId});await f.ok('B','sop_dispatch_share_accept',{pair_id:pair.pair.id});
 const permanent=(await f.ok('office','sop_attendance_register',{name:'合成长短工牌关联',agency:'가온'})).person;
 await f.ok('office','sop_daily_dispatch_update',{person_id:permanent.id,badge:permanent.badgeId,enabled:true,departments:['bulk'],version:0,reason:'合成授权'});
 const daily=(await f.ok('office','sop_attendance_checkin',{name:'合成临时关联人员',agency:'가온'})).record;
 await f.ok('office','sop_daily_dispatch_link',{person_id:permanent.id,badge:permanent.badgeId,daily_badge:daily.badgeId,daily_person_id:daily.personId,attendance_id:daily.id,reason:'合成本人关联'});
 const plan=await f.ok('office','v2_inbound_plan_create',{customer:'合成重置入库客户',biz_classes:['bulk']}),need=await f.ok('office','sop_need_create',{department:'bulk',source_type:'inbound',source_id:plan.id,customer:'合成重置入库客户',title:'合成资料重置',instructions:'仅本地合成资料',planned_quantity:2,planned_unit:'箱'}),form=new FormData();
 for(const [key,value]of Object.entries({action:'v2_attachment_upload',related_doc_type:'inbound_plan',related_doc_id:plan.id,need_id:need.id,attachment_category:'batch_work_material',client_req_id:crypto.randomUUID()}))form.set(key,value);form.set('file',new File(['synthetic reset material'],'合成重置资料.csv',{type:'text/csv'}));
 const upload=await(await f.worker.fetch(new Request('http://127.0.0.1/api',{method:'POST',headers:{Cookie:f.cookies.office,...operationHeaders(f.cookies.office)},body:form}),f.env)).json();assert.equal(upload.ok,true,upload.error);
 await f.ok('office','sop_batch_work_material_remove',{id:need.id,attachment_id:upload.id,material_revision:0});
 f.DB.raw.prepare("INSERT INTO v2_003_locations(id,location_name) VALUES('SYN-RESET-KEEP','合成保留库位')").run();
 if(process.env.CK_P1_NATIVE_D1==='true'){native=await localD1(f.DB.raw);f.env.DB=native;}
 const query=(sql,...args)=>f.env.DB.prepare(sql).bind(...args),rows=async(sql,...args)=>(await query(sql,...args).all()).results;
 const reset=async(action,body={},changes={})=>{
  const response=await entry.fetch(new Request('https://'+STAGING_HOST+'/api/test-data/'+action,{method:action==='status'?'GET':'POST',headers:{Cookie:f.cookies.office,Origin:'https://'+STAGING_HOST,'Content-Type':'application/json','X-CK-Test-Reset':'1'},...(action==='status'?{}:{body:JSON.stringify(body)})}),{...f.env,...changes});
  return {status:response.status,result:await response.json()};
 };
 const okReset=async(action,body)=>{const {status,result}=await reset(action,body);assert.equal(result.ok,true,action+': '+JSON.stringify({status,result}));return result;};
 const complete=async run=>{let result;for(let i=0;i<100;i++){result=await okReset('step',{runId:run.id});if(result.run.completedAt)return result;}assert.fail('Synthetic reset did not complete in bounded steps');};
 return {...f,first,second,plan,objects,query,rows,reset,okReset,complete};
}
