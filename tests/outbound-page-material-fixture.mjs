// Synthetic authenticated API fixture. No remote data, files or business writes.
import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {ownedHomeFixture} from './field-owned-home-fixture.mjs';import {localD1} from './d1-local-runtime.mjs';
import defaultWorker from '../worker-v2/index.js';import {operationHeaders,rememberOperation} from './office-operator-fixture.mjs';
export const outboundHash=x=>createHash('sha256').update(typeof x==='string'?x:JSON.stringify(x)).digest('hex');
export const outboundCases=[
 ['default',{}],['second-page',{offset:50}],['last-page',{offset:59,limit:5}],['past-end',{offset:9999}],['one',{limit:1}],['max',{limit:9999}],['coerced-page',{limit:'7items',offset:-2}],
 ['date-range',{start_date:'2026-11-03',end_date:'2026-11-05'}],['order-date',{date_basis:'order_date',start_date:'2026-11-02',end_date:'2026-11-03'}],['unknown-date-basis',{date_basis:'legacy'}],
 ['cancelled',{status:'cancelled'}],['accounted-0',{accounted:0}],['accounted-1',{accounted:1}],['stock-0',{uses_stock_operation:0}],['stock-1',{uses_stock_operation:1}],['bulk',{biz_class:'bulk'}],['customer',{customer_keyword:'客户김1'}],
 ['combined',{biz_class:'bulk',accounted:'0',uses_stock_operation:'0',start_date:'2026-11-01',end_date:'2026-11-05',limit:3,offset:1}],['empty',{customer_keyword:'SYN-ABSENT'}],['has-1',{has_material:'1'}],['has-0',{has_material:'0'}],['has-unrecognized',{has_material:'legacy'}]
];
export function publishedOutboundSql(sql){
 if(!sql.startsWith('SELECT * FROM (WITH outbound_page AS MATERIALIZED ('))return sql;
 const prefix='SELECT * FROM (WITH outbound_page AS MATERIALIZED (',middle=') SELECT v2_outbound_orders.*, ',suffix=' AS material_count FROM outbound_page AS v2_outbound_orders) ORDER BY ';
 const split=sql.indexOf(middle),last=sql.lastIndexOf(suffix);assert.ok(split>0&&last>split);const page=sql.slice(prefix.length,split),expression=sql.slice(split+middle.length,last);assert.ok(page.startsWith('SELECT * FROM v2_outbound_orders'));
 return 'SELECT *, '+expression+' AS material_count'+page.slice('SELECT *'.length);
}
export async function outboundFixture(t,{worker=defaultWorker,orders=65,history=0,sequential=false,native=process.env.CK_OUTBOUND_PAGE_NATIVE_D1==='true'}={}){
 t?.mock?.timers.enable({apis:['Date'],now:Date.parse('2026-11-03T00:00:00Z')});const f=await ownedHomeFixture();let runtime;
 f.callTarget=async(scope,action,body={})=>{const response=await worker.fetch(new Request('http://fixture.local'+(scope==='office'?'/api':'/001/api'),{method:'POST',headers:{'Content-Type':'application/json',Cookie:f.cookies[scope]||'',...operationHeaders(f.cookies[scope])},body:JSON.stringify({action,client_req_id:crypto.randomUUID(),...body})}),f.env);return rememberOperation(await response.json(),f.cookies[scope]);};
 f.okTarget=async(scope,action,body={})=>{const r=await f.callTarget(scope,action,body);assert.equal(r.ok,true,action+': '+r.error);return r;};
 await f.okTarget('office','v2_outbound_order_list',{});
 const raw=f.DB.raw,oid=i=>'SYN-OUT-'+String(i).padStart(5,'0'),order=raw.prepare('INSERT INTO v2_outbound_orders(id,display_no,customer,biz_class,status,order_date,expected_ship_at,accounted,uses_stock_operation,created_at,updated_at,warehouse_ack_required,revision_no) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)');
 raw.exec('BEGIN');
 for(let i=0;i<orders;i++){
  const at=new Date(Date.parse('2026-11-03T00:00:00Z')+(sequential?i:Math.floor(i/2))*1000).toISOString();
  const tie=!sequential&&i%10<2;
  order.run(oid(i),'SYN-NO-'+i,'合成客户김'+i%3,['bulk','direct_ship','return'][i%3],sequential?'pending_issue':i%13===0?'cancelled':['pending_issue','issued','ready_to_ship','shipped'][i%4],tie?'2026-11-02':'2026-11-0'+(i%3+1),sequential||tie?'2026-11-03':i%4===0?'':i%4===1?null:'2026-11-0'+(i%5+1)+'T08:00',i%3===0?null:i%2,i%3===0?null:i%2,at,at,!sequential&&i%4===0?1:0,2);
 }
 const change=raw.prepare('INSERT INTO v2_outbound_order_change_logs(id,order_id,revision_no,summary_text,changed_at,warehouse_ack_required) VALUES(?,?,?,?,?,1)');
 for(let i=0;i<orders;i+=4){change.run('SYN-CHANGE-1-'+i,oid(i),1,'合成旧摘要'+i,'2026-11-02T00:00:00Z');change.run('SYN-CHANGE-2-'+i,oid(i),2,i%8?'合成最新摘要'+i:'','2026-11-03T00:00:00Z');}
 const need=raw.prepare("INSERT INTO sop_records(id,kind,department,revision,state,updated_at) VALUES(?,'need','bulk',1,?,'2026-11-03T00:00:00Z')"),att=raw.prepare('INSERT INTO v2_attachments(id,related_doc_type,related_doc_id,attachment_category,file_key,file_name) VALUES(?,?,?,?,?,?)');
 f.oid=oid;f.seedAttachment=(id,type,doc,category='work_material',key='synthetic/shared')=>att.run(id,type,doc,category,key,'合成资料'+id+'.pdf');
 f.seedNeed=(id,data)=>need.run(id,JSON.stringify(data));
 for(let i=0;i<orders;i+=7){f.seedAttachment('SYN-DIRECT-'+i,'outbound_order',oid(i),'outbound_material');f.seedAttachment('SYN-NONMATERIAL-'+i,'outbound_order',oid(i),'arrival_photo');}
 f.seedAttachment('SYN-OWN','sop_need','SYN-NEED-1');f.seedAttachment('SYN-WITHDRAWN','sop_need','SYN-NEED-1');f.seedAttachment('SYN-INBOUND','inbound_plan','SYN-IB','inbound_material');f.seedAttachment('SYN-BATCH','inbound_plan','SYN-IB','batch_work_material');
 f.seedNeed('SYN-NEED-1',{source_type:'inbound',source_id:'SYN-IB',links:[{outbound_id:oid(1)},{outbound_id:oid(2)},{outbound_id:oid(1)}],removed_material_ids:['SYN-WITHDRAWN']});
 f.seedNeed('SYN-NEED-2',{source_type:'inbound',source_id:'SYN-IB',links:[{outbound_id:oid(1)}],status:'closed'});
 f.seedNeed('SYN-NEED-3',{source_type:'outbound',source_id:oid(3),links:[],status:'cancelled'});f.seedAttachment('SYN-LEGACY','sop_need','SYN-NEED-3');
 for(const id of ['SYN-GROUP-1','SYN-GROUP-2','SYN-COMMON','SYN-UNSELECTED'])f.seedAttachment(id,'sop_need','SYN-GROUP');
 f.seedNeed('SYN-GROUP',{cargo_groups:{groups:[]},links:[{outbound_id:oid(4),cargo_allocations:[{documents:[{id:'SYN-GROUP-1'},{id:'SYN-GROUP-1'},{id:'SYN-MISSING'}],public_documents:[{id:'SYN-COMMON'}]}]},{outbound_id:oid(5),cargo_reservation:{groups:[{documents:[{id:'SYN-GROUP-2'}],public_documents:[{id:'SYN-COMMON'}]}]}}],removed_material_ids:['SYN-GROUP-1']});
 f.seedNeed('SYN-JSON-NULL',{cargo_groups:null,source_type:'outbound',source_id:oid(6),links:[]});f.seedAttachment('SYN-JSON-NULL-FILE','sop_need','SYN-JSON-NULL');
 for(let i=0;i<history;i++){f.seedNeed('SYN-HISTORY-'+i,{source_type:'inbound',source_id:'SYN-HISTORY-IB-'+i,links:[{outbound_id:'SYN-UNRELATED-'+i}]});for(let j=0;j<5;j++)f.seedAttachment('SYN-HISTORY-ATT-'+i+'-'+j,'sop_need','SYN-HISTORY-'+i);}
 raw.prepare("INSERT INTO ck_batch_material_state VALUES('SYN-BATCH','SYN-IB',1,1,'synthetic','合成核查员','2026-11-03T00:00:00Z')").run();raw.exec('COMMIT');
 f.close=async()=>{await runtime?.close();raw.close();};t?.after(()=>f.close());
 if(native){runtime=await localD1(raw);f.env.DB=runtime;await f.okTarget('office','v2_outbound_order_list',{customer_keyword:'SYN-NONEXISTENT'});}
 f.rows=async(sql,...args)=>(await f.env.DB.prepare(sql).bind(...args).all()).results;
 f.run=(sql,...args)=>f.env.DB.prepare(sql).bind(...args).run();
 f.snapshot=async()=>outboundHash(await Promise.all(['sop_records','sop_events','sop_document_numbers','v2_outbound_orders','v2_outbound_order_change_logs','v2_attachments','ck_batch_material_state','v2_ops_jobs','v2_ops_job_workers','v2_ops_job_results'].map(table=>f.rows('SELECT * FROM '+table+' ORDER BY rowid'))));
 const batch=f.env.DB.batch.bind(f.env.DB);let capturing;
 f.env.DB.batch=async ss=>{if(capturing&&ss[0]?.sql.startsWith('SELECT COUNT(*) AS c FROM v2_outbound_orders'))capturing.push(ss.map(s=>({sql:s.sql,args:[...s.args]})));return batch(ss);};
 f.read=async(body={},scope='office')=>{capturing=[];try{const start=performance.now(),r=await f.okTarget(scope,'v2_outbound_order_list',body),ms=performance.now()-start;return {r,ms,queries:capturing};}finally{capturing=null;}};
 return f;
}
