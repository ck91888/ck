import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const ui=readFileSync(new URL('../shared/sop-planning-ui.js',import.meta.url),'utf8');
const app=readFileSync(new URL('../002/app.js',import.meta.url),'utf8');
function setup(){const context=vm.createContext({Event});context.window=context;vm.runInContext(ui,context);return context;}
const blankLegacy={seq:'1',customer:'',biz_class:'bulk',outbound_mode:'',uses_stock_operation:0,expected_ship_at:'',destination:'',po_no:'',wms_work_order_no:'',planned_box_count:0,planned_pallet_count:0,outbound_requirement:'',instruction:'',remark:''};
const need={title:'BD001～015打托',instructions:'反馈明细后客户再预约',outbounds:[]};
function readOptional(context,data){
 const button={},host={innerHTML:'',querySelector:()=>button,querySelectorAll:()=>data.map(row=>({querySelectorAll:()=>Object.entries(row).map(([key,value])=>({dataset:{ob:key},value:String(value)}))}))};
 return context.CKOptionalOutbounds(host)();
}
test('unopened and untouched optional outbound rows yield no shipping plan',()=>{
 const context=setup();assert.equal(readOptional(context,[]).length,0);
 assert.equal(readOptional(context,[{expected_ship_at:'',quantity:'',outbound_mode:'customer_pickup',destination:'',outbound_requirement:''}]).length,0);
 assert.equal(context.CKHasStandaloneOutbound(blankLegacy),false);
 for(const change of [{destination:'仁川'},{outbound_mode:'customer_pickup'},{planned_box_count:15},{remark:'保留资料'},{biz_class:'direct_ship'}])assert.equal(context.CKHasStandaloneOutbound({...blankLegacy,...change}),true);
});
test('partially entered outbound data is never silently discarded',()=>{
 const context=setup();
 for(const row of [{destination:'仁川'},{quantity:'15'},{expected_ship_at:'2026-09-25'},{outbound_mode:'warehouse_dispatch'},{expected_ship_at:'2026-09-25',quantity:'NaN'}])assert.throws(()=>readOptional(context,[row]),/补齐出库日期和分配数量/);
 const result=readOptional(context,[{expected_ship_at:'2026-09-25',quantity:'15',outbound_mode:'customer_pickup'}]);assert.equal(result.length,1);assert.equal(result[0].quantity,'15');
});
async function submit({works=[need],checked=true,rows=[blankLegacy],helper=true}={}){
 const context=setup(),requests=[],alerts=[],fields={'ibc-customer':{value:'宝袋'},'ibc-link-ob':{checked}};
 Object.assign(context,{document:{getElementById:id=>fields[id]||{value:''}},CKInboundWorks:{read:()=>works},getIbcBizClasses:()=>['bulk'],getIbcLines:()=>[{unit_type:'box',planned_qty:50}],getIbcLinkObRows:()=>rows,kstToday:()=> '2026-09-22',unitTypeLabel:()=> '箱',getUser:()=> '测试负责人',alert:m=>alerts.push(m),api:async payload=>{requests.push(payload);return {ok:false,error:'fixture: stop after request capture'};},withActionLock:(_key,_btn,_text,fn)=>{context.pending=fn();}});
 if(!helper)context.CKHasStandaloneOutbound=undefined;
 vm.runInContext(app.slice(app.indexOf('async function submitInbound('),app.indexOf('function openInboundDetail(')),context);
 await context.submitInbound();await context.pending;return {requests,alerts};
}
test('actual inbound submit ignores stale checked empty legacy panel and retains the need',async()=>{
 for(const rows of [[],[blankLegacy]]){const r=await submit({rows});assert.equal(r.requests.length,1);assert.equal(r.requests[0].action,'v2_inbound_plan_create');assert.equal(r.requests[0].lines[0].planned_qty,50);assert.equal(r.requests[0].work_requests[0].outbounds.length,0);}
});
test('filled legacy draft remains protected until explicitly unselected or assigned to a need',async()=>{
 const rows=[{...blankLegacy,outbound_mode:'customer_pickup',planned_box_count:15}];
 const blocked=await submit({rows});assert.equal(blocked.requests.length,0);assert.match(blocked.alerts[0],/尚未归属/);
 const deferred=await submit({rows,checked:false});assert.equal(deferred.requests.length,1);
 const standalone=await submit({rows,works:[]});assert.equal(standalone.requests.length,1);
 const production=await submit({helper:false,works:[],rows:[]});assert.equal(production.requests.length,0,'legacy mode unchanged without the staging helper');
});
test('work-form switches away from empty duplicate outbound entry and preserves edited drafts',()=>{
 const context=setup(),listeners={},checkbox={checked:true,addEventListener:(name,fn)=>listeners[name]=fn},group={append(){}},label={childNodes:[checkbox],append(){}},panel={style:{},addEventListener(){}};
 checkbox.closest=selector=>selector==='label'?label:group;
 const list={children:[{}]},host={querySelector:()=>list,addEventListener:(name,fn)=>listeners[name]=fn};let rows=[blankLegacy];
 context.document={getElementById:id=>id==='ibc-link-ob'?checkbox:panel,createElement:()=>({}),createTextNode:text=>({text})};context.getIbcLinkObRows=()=>rows;
 context.CKConnectInboundOutbounds(host);assert.equal(group.hidden,true);assert.equal(checkbox.checked,false);assert.equal(panel.style.display,'none');
 list.children=[];listeners['ck-work-change']();assert.equal(group.hidden,false);
 rows=[{...blankLegacy,destination:'仁川'}];checkbox.checked=true;list.children=[{}];listeners['ck-work-change']();assert.equal(group.hidden,false);assert.equal(checkbox.checked,true);assert.equal(panel.style.display,'');assert.equal(rows[0].destination,'仁川');
});
