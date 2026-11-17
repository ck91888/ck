import test from 'node:test';import assert from 'node:assert/strict';
import worker from '../worker-v2/index.js';import {database} from './d1-adapter.mjs';
async function fixture(){
 const DB=database(),env={DB,SOP_ENVIRONMENT:'staging',SOP_UPGRADE_ENABLED:'true',SOP_PUBLIC_TEST_ACCESS:'true'};
 const request=async(action,data={})=>worker.fetch(new Request('https://fixture.local/api',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...data})}),env);
 const call=async(action,data={})=>{const r=await(await request(action,data)).json();assert.equal(r.ok,true,r.error||r.message);return r;};
 const plan=async(arrival,extra={})=>{const p=await call('v2_inbound_plan_create',{customer:'QA 合成客户',biz_classes:['bulk'],expected_arrival:arrival,lines:[{unit_type:'carton',planned_qty:1}],...extra});return DB.raw.prepare('SELECT * FROM v2_inbound_plans WHERE id=?').get(p.id);};
 return {DB,request,call,plan,list:b=>call('v2_inbound_plan_list',b),export:b=>call('v2_inbound_plan_export',b)};
}
const ids=r=>r.items.map(p=>p.id).sort();
test('expected arrival filters the business field independently of creation and actual unloading',async()=>{
 const f=await fixture();try{
 const a=await f.plan('2026-10-13'),b=await f.plan('2026-10-14');
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-14',created_at='2026-10-14T00:00:00Z',unload_completed_at='2026-10-14T00:00:00Z' WHERE id=?").run(a.id);
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-13',created_at='2026-10-13T00:00:00Z',unload_completed_at='2026-10-13T00:00:00Z' WHERE id=?").run(b.id);
 assert.deepEqual(ids(await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'})),[a.id]);
 }finally{f.DB.raw.close();}
});
test('inclusive range, one-sided ranges, undated exclusion and clearing retain the original data',async()=>{
 const f=await fixture();try{
 const plans=[];for(const d of ['2026-10-12','2026-10-13','2026-10-14','2026-10-15',''])plans.push(await f.plan(d));
 f.DB.raw.prepare('UPDATE v2_inbound_plans SET expected_arrival=NULL WHERE id=?').run(plans[4].id);
 const before=f.DB.raw.prepare('SELECT id,expected_arrival FROM v2_inbound_plans ORDER BY id').all();
 assert.deepEqual(ids(await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-14'})),plans.slice(1,3).map(p=>p.id).sort());
 assert.equal((await f.list({expected_arrival_from:'2026-10-14'})).total,2);assert.equal((await f.list({expected_arrival_to:'2026-10-13'})).total,2);
 assert.equal((await f.list({expected_arrival_from:'',expected_arrival_to:''})).total,5);
 assert.deepEqual(f.DB.raw.prepare('SELECT id,expected_arrival FROM v2_inbound_plans ORDER BY id').all(),before);
 }finally{f.DB.raw.close();}
});
test('Korean business dates agree with existing normalization for legacy dates and time-zone boundaries',async()=>{
 const f=await fixture();try{
 const values=['2026-10-12T14:59:59Z','2026-10-12T15:00:00Z','2026-10-13T14:59:59.999Z','2026-10-13T15:00:00Z','2026-10-13T23:59:59.999','2026/10/13','2026-10-12T23:00:00-04:00','2026-10-14T01:00:00+12:00'];
 const plans=[];for(const value of values){const p=await f.plan(value);f.DB.raw.prepare('UPDATE v2_inbound_plans SET expected_arrival=? WHERE id=?').run(value,p.id);plans.push(p);}
 const result=await f.list({expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'});assert.deepEqual(ids(result),[1,2,4,5,6,7].map(i=>plans[i].id).sort());
 const api=await f.plan('2026-10-12T15:00:00Z');assert.equal(api.expected_arrival,'2026-10-13');
 }finally{f.DB.raw.close();}
});
test('customer, business, status, accounting, creation and unloading compose with page totals and CSV export',async()=>{
 const f=await fixture();try{
 const targets=[];for(let i=0;i<3;i++)targets.push(await f.plan('2026-10-13'));
 await f.plan('2026-10-14');await f.plan('2026-10-13',{customer:'QA 其他客户'});await f.plan('2026-10-13',{biz_classes:['return']});const complete=await f.plan('2026-10-13');f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='completed' WHERE id=?").run(complete.id);
 for(const p of targets)f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-01',accounted=1,unload_completed_at='2026-10-12T15:00:00Z' WHERE id=?").run(p.id);
 const filter={expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13',customer_keyword:'合成',biz_class:'bulk',status:'pending',accounted:'1',start_date:'2026-10-01',end_date:'2026-10-01',unload_done_date_from:'2026-10-13',unload_done_date_to:'2026-10-13',limit:2};
 const first=await f.list(filter),second=await f.list({...filter,offset:2});assert.equal(first.total,3);assert.equal(first.page_count,2);assert.equal(first.items.length,2);assert.equal(second.total,3);assert.equal(second.items.length,1);assert.deepEqual([...ids(first),...ids(second)].sort(),targets.map(p=>p.id).sort());
 const exported=await f.export({...filter,limit:10000});assert.equal(exported.rows.length,3);assert.deepEqual(exported.rows.map(p=>p['入库单号']).sort(),targets.map(p=>p.display_no).sort());assert.ok(exported.rows.every(p=>p['预计到达日期']==='2026-10-13'));
 }finally{f.DB.raw.close();}
});
test('invalid dates or a reversed range fail both list and export instead of silently widening the search',async()=>{
 const f=await fixture();try{await f.plan('2026-10-13');for(const data of [{expected_arrival_from:'2026-02-30'},{expected_arrival_to:'2026-13-01'},{expected_arrival_from:'2026-10-14',expected_arrival_to:'2026-10-13'},{expected_arrival_from:"2026-10-13' OR 1=1"}])for(const action of ['v2_inbound_plan_list','v2_inbound_plan_export']){const r=await f.request(action,data);assert.equal(r.status,400);assert.equal((await r.json()).ok,false);}assert.equal((await f.list({})).total,1);
 }finally{f.DB.raw.close();}
});
test('arrival filtering preserves cancelled, deleted and return-session exclusions',async()=>{
 const f=await fixture();try{
 const good=await f.plan('2026-10-13'),cancelled=await f.plan('2026-10-13'),deleted=await f.plan('2026-10-13'),session=await f.plan('2026-10-13');
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET status='cancelled' WHERE id=?").run(cancelled.id);f.DB.raw.prepare('UPDATE v2_inbound_plans SET is_deleted=1 WHERE id=?').run(deleted.id);f.DB.raw.prepare("UPDATE v2_inbound_plans SET source_type='return_session' WHERE id=?").run(session.id);
 const b={expected_arrival_from:'2026-10-13',expected_arrival_to:'2026-10-13'};assert.deepEqual(ids(await f.list(b)),[good.id]);assert.equal((await f.export(b)).rows.length,1);assert.deepEqual(ids(await f.list({...b,status:'cancelled'})),[cancelled.id]);
 }finally{f.DB.raw.close();}
});

test('arrival order uses the existing KST calendar day for legacy formats, with stable ties and undated rows last',async()=>{
 const f=await fixture();try{
 // The within-day clock is intentionally not a secondary sort: this field is a business date.
 const values=[['utc-next','2026-10-13T15:00:00Z','2026-10-01'],['iso-day','2026-10-13','2026-10-06'],
  ['local-late','2026-10-13T23:59:59.999','2026-10-05'],['slash','2026/10/13','2026-10-04'],
  ['utc-start','2026-10-12T15:00:00Z','2026-10-03'],['negative-offset','2026-10-12T23:00:00-04:00','2026-10-02'],
  ['positive-offset','2026-10-14T01:00:00+12:00','2026-10-01'],['utc-before','2026-10-12T14:59:59Z','2026-10-31'],
  ['empty','','2026-10-31'],['null',null,'2026-10-30'],['spaces','   ','2026-10-29'],['invalid','unknown','2026-10-28']];
 const plans=[];for(const [name,value,day] of values){const p=await f.plan('');f.DB.raw.prepare('UPDATE v2_inbound_plans SET expected_arrival=?,plan_date=?,customer=?,display_no=? WHERE id=?').run(value,day,name,'QA-SORT-'+p.id,p.id);p.display_no='QA-SORT-'+p.id;plans.push(p);}
 const before=f.DB.raw.prepare('SELECT * FROM v2_inbound_plans ORDER BY id').all();
 assert.deepEqual((await f.list({})).items.map(p=>p.id),plans.map(p=>p.id));
 const exported=await f.export({});assert.deepEqual(exported.rows.map(p=>p['入库单号']),plans.map(p=>p.display_no));
 assert.deepEqual(exported.rows.slice(0,8).map(p=>p['预计到达日期']),['2026-10-14',...Array(6).fill('2026-10-13'),'2026-10-12']);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plans ORDER BY id').all(),before);
 // Every final tie is resolved by ID, regardless of physical insertion order.
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET expected_arrival='2026-10-13',plan_date='2026-10-01',created_at='2026-10-01T00:00:00Z'").run();
 const tied=plans.map(p=>p.id).sort().reverse();
 assert.deepEqual((await f.list({})).items.map(p=>p.id),tied);
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET created_at='2026-10-02T00:00:00Z' WHERE id=?").run(tied.at(-1));
 assert.equal((await f.list({limit:1})).items[0].id,tied.at(-1));
 }finally{f.DB.raw.close();}
});

test('88 plans are globally ordered before pagination and CSV limits without widening filters or page enrichment',async()=>{
 const f=await fixture();try{
 const targets=[];for(let i=0;i<88;i++){
  const day=i<85?'2026-10-'+String(1+(i*7)%28).padStart(2,'0'):'';
  const p=await f.plan(day);f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-01',created_at=?,accounted=1,unload_completed_at='2026-10-12T15:00:00Z',display_no=? WHERE id=?").run('2026-10-01T00:00:'+String(i%60).padStart(2,'0')+'Z','QA-SORT-'+p.id,p.id);p.display_no='QA-SORT-'+p.id;
  targets.push({...p,expected_arrival:day,created_at:'2026-10-01T00:00:'+String(i%60).padStart(2,'0')+'Z'});
 }
 for(const changes of ["status='cancelled'","is_deleted=1","source_type='return_session'","customer='Excluded'","biz_class='return',biz_classes_json='[\"return\"]'","status='completed'","accounted=0","plan_date='2026-10-02'","unload_completed_at='2026-10-13T15:00:00Z'"]){
  const p=await f.plan('2026-12-31');f.DB.raw.prepare("UPDATE v2_inbound_plans SET accounted=1,plan_date='2026-10-01',unload_completed_at='2026-10-12T15:00:00Z',display_no=? WHERE id=?").run('QA-EXCLUDED-'+p.id,p.id);
  f.DB.raw.prepare('DELETE FROM v2_inbound_plan_biz_tasks WHERE plan_id=?').run(p.id);f.DB.raw.prepare('UPDATE v2_inbound_plans SET '+changes+' WHERE id=?').run(p.id);
 }
 const compare=(a,b)=>a<b?1:a>b?-1:0;
 const ordered=targets.sort((a,b)=>compare(a.expected_arrival,b.expected_arrival)||compare(a.created_at,b.created_at)||compare(a.id,b.id));
 const filter={customer_keyword:'合成',biz_class:'bulk',status:'pending',accounted:'1',start_date:'2026-10-01',end_date:'2026-10-01',unload_done_date_from:'2026-10-13',unload_done_date_to:'2026-10-13'};
 // Warm schema initialization before observing read-only query count and row bounds.
 await f.list(filter);const prepare=f.DB.prepare.bind(f.DB),reads=[];
 f.DB.prepare=sql=>{const s=prepare(sql);for(const method of ['all','first','run']){const original=s[method].bind(s);s[method]=async()=>{const r=await original();reads.push({sql,args:s.args,rows:r?.results?.length});return r;};}return s;};
 const before=f.DB.raw.prepare('SELECT * FROM v2_inbound_plans ORDER BY id').all();
 const first=await f.list(filter),firstReads=reads.splice(0),second=await f.list({...filter,offset:50});
 assert.equal(first.limit,50);assert.equal(first.total,88);assert.equal(first.page_count,2);assert.equal(second.total,88);
 assert.deepEqual([...first.items,...second.items].map(p=>p.id),ordered.map(p=>p.id));
 assert.deepEqual((await f.list(filter)).items.map(p=>p.id),first.items.map(p=>p.id));
 assert.equal((await f.list({...filter,offset:100})).items.length,0);
 const smaller=[];for(let offset=0;offset<88;offset+=20)smaller.push(...(await f.list({...filter,limit:20,offset})).items);
 assert.deepEqual(smaller.map(p=>p.id),ordered.map(p=>p.id));
 const exportAll=await f.export(filter),exportFirst=await f.export({...filter,limit:50});
 assert.deepEqual(exportAll.rows.map(p=>p['入库单号']),ordered.map(p=>p.display_no));
 assert.deepEqual(exportFirst.rows.map(p=>p['入库单号']),ordered.slice(0,50).map(p=>p.display_no));
 const range={...filter,expected_arrival_from:'2026-10-10',expected_arrival_to:'2026-10-24',limit:200};
 const expected=ordered.filter(p=>p.expected_arrival>='2026-10-10'&&p.expected_arrival<='2026-10-24');
 assert.deepEqual((await f.list(range)).items.map(p=>p.id),expected.map(p=>p.id));
 assert.deepEqual((await f.export(range)).rows.map(p=>p['入库单号']),expected.map(p=>p.display_no));
 assert.equal((await f.list({...filter,limit:10000})).limit,200);
 assert.deepEqual(f.DB.raw.prepare('SELECT * FROM v2_inbound_plans ORDER BY id').all(),before);
 assert.ok(firstReads.length<=10,JSON.stringify(firstReads));
 assert.ok(firstReads.every(r=>!/^\s*(UPDATE|INSERT|DELETE|CREATE|ALTER)/i.test(r.sql)));
 assert.equal(firstReads.find(r=>r.sql.startsWith('SELECT * FROM v2_inbound_plans')).rows,50);
 assert.ok(firstReads.filter(r=>r.sql.includes(' IN (')).every(r=>r.args.length<=50),'only selected page is enriched');
 }finally{f.DB.raw.close();}
});

test('execution candidate and legacy required-business list ordering remain unchanged',async()=>{
 const f=await fixture();try{
 const old=await f.plan('2026-10-22'),recent=await f.plan('2026-10-16');
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-01',created_at='2026-10-01T00:00:00Z' WHERE id=?").run(old.id);
 f.DB.raw.prepare("UPDATE v2_inbound_plans SET plan_date='2026-10-02',created_at='2026-10-02T00:00:00Z' WHERE id=?").run(recent.id);
 assert.deepEqual((await f.list({})).items.map(p=>p.id),[old.id,recent.id]);
 assert.deepEqual((await f.list({required_biz_class:'bulk'})).items.map(p=>p.id),[recent.id,old.id]);
 assert.deepEqual((await f.call('v2_inbound_plan_ops_candidates',{scene:'unload'})).items.map(p=>p.id),[recent.id,old.id]);
 }finally{f.DB.raw.close();}
});
