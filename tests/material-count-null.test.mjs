// Real authenticated Worker API with synthetic memory/local workerd data only.
import test from 'node:test';
import assert from 'node:assert/strict';
import {outboundFixture} from './outbound-page-material-fixture.mjs';

async function fixture(t){
 const f=await outboundFixture(t,{orders:10,native:process.env.CK_MATERIAL_NULL_NATIVE_D1==='true'}),id=f.oid(6),need='SYN-JSON-NULL';
 const state=value=>({status:'pending',source_type:'outbound',source_id:id,links:[],...(value===undefined?{}:{cargo_groups:value})});
 const set=value=>f.run('UPDATE sop_records SET state=? WHERE id=?',JSON.stringify(state(value)),need);
 const call=(action,body={})=>f.okTarget('office',action,body);
 const check=async(count,{readDetail=true}={})=>{
  const before=await f.snapshot(),list=await call('v2_outbound_order_list',{limit:200}),has=await call('v2_outbound_order_list',{limit:200,has_material:'1'}),none=await call('v2_outbound_order_list',{limit:200,has_material:'0'});
  assert.equal(list.items.find(x=>x.id===id).material_count,count);assert.equal(has.items.some(x=>x.id===id),count>0);assert.equal(none.items.some(x=>x.id===id),count===0);
  assert.equal(has.total+none.total,list.total);assert.equal(new Set([...has.items,...none.items].map(x=>x.id)).size,list.total);
  if(readDetail){const d=await call('v2_outbound_order_detail',{id}),a=await call('v2_attachment_list',{related_doc_type:'outbound_order',related_doc_id:id});assert.equal(d.order.material_count,count);assert.equal(a.items.filter(x=>x.attachment_category==='outbound_material').length,count);}
  assert.equal(await f.snapshot(),before,'reading material metadata must not change business or audit rows');
 };
 return {...f,id,need,state,set,check,call};
}
for(const [name,value]of [['JSON null',null],['false',false],['numeric zero',0],['empty string','']])test(name+' has the same legacy files in list, detail, attachment and both filters',async t=>{
 const f=await fixture(t);await f.set(value);await f.check(1);
});
test('a missing grouping field keeps its original legacy material semantics',async t=>{
 const f=await fixture(t);await f.set(undefined);await f.check(1);
});
test('truthy malformed grouping values cannot silently expose unselected library files',async t=>{
 const f=await fixture(t);
 for(const value of [{},[],true,1,'null','0','{"groups":[]}']){await f.set(value);await f.check(0);const before=await f.snapshot(),r=await f.callTarget('office','sop_cargo_groups',{id:f.need});assert.equal(r.ok,false,'invalid grouping remains a structural error');assert.equal(await f.snapshot(),before);}
});
