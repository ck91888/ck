import {allProcessesApproved} from './cargo-processes.js';
// Read immutable allocation snapshots. Capacity is keyed by cargo group, never reset by a new output version.
export function cargoReady(data,orderId){
 if(!data.cargo_groups)return !!data.result&&data.status!=='cancelled';
 const link=(data.links||[]).find(l=>l.outbound_id===orderId),allocations=link?.cargo_allocations;
 return data.status!=='cancelled'&&Array.isArray(allocations)&&allocations.length>0&&allocations.every(a=>{
  const group=data.cargo_groups.groups.find(g=>g.id===a.group_id),output=group?.outputs?.find(o=>o.id===a.output_id&&o.version===a.output_version);
  return allProcessesApproved(group||{})&&output?.status==='approved'&&output.quantity===a.quantity&&output.unit===a.unit&&output.mapping_version===a.mapping_version;
 });
}
export function cargoFiles(data,orderId){
 const link=(data.links||[]).find(l=>l.outbound_id===orderId);return [...new Map((link?.cargo_allocations||link?.cargo_reservation?.groups||[]).flatMap(a=>[...(a.documents||[]),...(a.public_documents||[])]).map(f=>[f.id,f])).values()];
}
export async function cargoAvailability(env,data){
 const ids=[...new Set((data.links||[]).map(l=>l.outbound_id))],orders=ids.length?(await env.DB.prepare('SELECT id,status,display_no,expected_ship_at,outbound_mode,destination,po_no,outbound_requirement,updated_at FROM v2_outbound_orders WHERE id IN (SELECT value FROM json_each(?))').bind(JSON.stringify(ids)).all()).results:[];
 const byId=new Map(orders.map(o=>[o.id,o])),used=new Map();
 // Missing order is not permission to reuse its allocation. Shipped allocations remain consumed.
 for(const link of data.links||[])if(byId.get(link.outbound_id)?.status!=='cancelled')for(const a of link.cargo_allocations||[])used.set(a.group_id,(used.get(a.group_id)||0)+Number(a.quantity));
 const reserved=new Set((data.links||[]).filter(l=>l.cargo_reservation&&byId.get(l.outbound_id)?.status!=='cancelled').flatMap(l=>l.cargo_reservation.groups.map(g=>g.group_id)));
 return {orders,reservations:(data.links||[]).filter(l=>l.cargo_reservation).map(l=>({outbound_id:l.outbound_id,...l.cargo_reservation,...(byId.get(l.outbound_id)?Object.fromEntries(['expected_ship_at','outbound_mode','destination','po_no','outbound_requirement'].map(k=>[k,byId.get(l.outbound_id)[k]])):{}),order:byId.get(l.outbound_id)})),groups:data.cargo_groups.groups.map(g=>{const o=g.outputs?.find(x=>x.id===g.current_output_id);return {...g,output:o||null,allocated:used.get(g.id)||0,reserved:reserved.has(g.id),available:!reserved.has(g.id)&&allProcessesApproved(g)&&o?.status==='approved'&&!(used.get(g.id)>0)};})};
}

export function cargoReferencesFile(data,id){
 const visit=v=>v&&typeof v==='object'&&Object.entries(v).some(([key,value])=>['documents','public_documents'].includes(key)?Array.isArray(value)&&value.some(f=>f.id===id):visit(value));
 return !!(visit(data.cargo_groups)||visit(data.links));
}
