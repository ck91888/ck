import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {workPlanFacts} from '../worker-v2/work-plan-list.js';
import {factsFixture} from './work-plan-performance-fixture.mjs';
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
// Frozen outputs and SQL arguments from the published f640c4f module, measured
// before this optimization. Array order and every field participate in the hash.
const published=[
 [100,'245578c278ac2f48a947f79f6eb27dc30bd0b5b22e7c9f57fab274e17c80e71f','daecfa75bf2241a1e733eefe4b83607d54656458f32b96b3cc9ea9336b891e0d'],
 [1000,'f1f89d95aa0fcfd974a72f6560c24598f78ebab6a0103a42f47971ccf8e8249d','90cbfd907326bc768463fe99adea1b8d4179ab33087e3fd6c6b183b0e887d07a'],
 [10000,'4754a87be434f5e123f1df54592408ad4272555f85d7d4000dd827470600b017','fef3814ef65c3d845a69d3c777d2754f2ee84b56ccc790cc741ea63e96570f00']
];
for(const [size,output,queries] of published)test('indexed work-plan facts match all published fields and queries at '+size+' groups',async()=>{
 const f=factsFixture(size);assert.equal(await workPlanFacts(f.env,f.groups,f.knownPlans,f.orders),f.groups);
 assert.equal(hash(f.groups),output);assert.equal(hash(f.reads),queries);
});
test('relationship union preserves original positions, duplicate IDs and strict source joins',async()=>{
 const orders=[
  {id:'DUP',source_inbound_plan_id:'P2',display_no:'first'},
  {id:'OTHER',source_inbound_plan_id:'P1',display_no:'source only'},
  {id:'DUP',source_inbound_plan_id:'P1',display_no:'repeated id'},
  {id:NaN,source_inbound_plan_id:NaN},
  {id:undefined,source_inbound_plan_id:undefined},
  {id:null,source_inbound_plan_id:null},
  {id:0,source_inbound_plan_id:0}
 ].map(x=>({status:'issued',expected_ship_at:'2026-11-05',...x}));
 const groups=[['inbound','P1'],['outbound','DUP'],['outbound',NaN],['inbound',NaN],['inbound',undefined],['inbound',null],['inventory',''],['outbound',0]].map(([source_type,source_id])=>({source_type,source_id,items:[{links:[{outbound_id:'DUP'},{outbound_id:'DUP'},{outbound_id:NaN},{outbound_id:undefined}]}]}));
 const expected=groups.map(g=>{const linked=new Set(g.items.flatMap(n=>n.links.map(l=>l.outbound_id)));if(g.source_type==='outbound')linked.add(g.source_id);const rows=orders.filter(o=>linked.has(o.id)||g.source_type==='inbound'&&o.source_inbound_plan_id===g.source_id);const origins=g.source_type==='inbound'?[g.source_id]:g.source_type==='outbound'?rows.filter(o=>o.id===g.source_id).map(o=>o.source_inbound_plan_id):[];return {rows,origins:[...new Set(origins.filter(Boolean))].filter(id=>['P1','P2'].includes(id))};});
 const empty={DB:{prepare(){return {bind(){return this;},async all(){return {results:[]};}};},async batch(){return [{results:[]},{results:[]}];}}};
 await workPlanFacts(empty,groups,[{id:'P1'},{id:'P2'}],orders);
 groups.forEach((g,i)=>{assert.deepEqual(g.timeline.shipments.map(o=>o.id),expected[i].rows.map(o=>o.id));assert.deepEqual(g.timeline.shipments.map(o=>o.display_no),expected[i].rows.map(o=>o.display_no||o.id));assert.deepEqual(g.timeline.plans.map(p=>p.id),expected[i].origins);});
});
test('indexing leaves original member/material objects and source arrays intact',async()=>{
 const f=factsFixture(30),before=structuredClone(f.groups),orders=structuredClone(f.orders),plans=structuredClone(f.knownPlans),members=f.groups.map(g=>g.items);
 await workPlanFacts(f.env,f.groups,f.knownPlans,f.orders);
 f.groups.forEach((g,i)=>{assert.equal(g.items,members[i]);const {timeline,...rest}=g;assert.deepEqual(rest,before[i]);});
 assert.deepEqual(f.orders,orders);assert.deepEqual(f.knownPlans,plans);
});
