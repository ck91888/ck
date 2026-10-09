import {allProcessesApproved} from './cargo-processes.js';
export const approvedOutput=g=>{const o=g.outputs?.find(x=>x.id===g.current_output_id);return o?.status==='approved'&&allProcessesApproved(g)?o:null;};
export const reservationAmount=(r,g,o=approvedOutput(g))=>r.schema===2?{quantity:r.quantity,unit:r.unit}:{quantity:o?.quantity||g.box_count,unit:o?.unit||'箱'};
// An order consumes capacity once: its frozen allocation, or its pending explicit quantity.
// Unknown quantities never imply all cargo and cannot make an order loadable.
export function cargoLedger(data,orders=[]){
 const byId=new Map(orders.map(o=>[o.id,o]));
 return data.cargo_groups.groups.map(g=>{const output=approvedOutput(g),unit=output?.unit||'箱',capacity=output?.quantity||g.box_count;let allocated=0,reserved=0,unknown=0;const scopes=[];
  for(const l of data.links||[]){if(byId.get(l.outbound_id)?.status==='cancelled')continue;const a=l.cargo_allocations?.find(a=>a.group_id===g.id),r=l.cargo_reservation;if(a){if(a.unit!==unit)throw Error('已分配成果单位已变化，请核对原出库记录');allocated+=a.quantity;if(a.shipping_marks?.length)scopes.push(a.shipping_marks);}
   else if(r?.groups.some(x=>x.group_id===g.id)){const amount=reservationAmount(r,g,output);if(amount.quantity!=null&&amount.unit===unit)reserved+=amount.quantity;else unknown++;if(r.shipping_marks?.length)scopes.push(r.shipping_marks);}
  }
  const seen=new Set();let overlap=false;for(const marks of scopes)for(const mark of marks){if(seen.has(mark))overlap=true;seen.add(mark);}
  return {group_id:g.id,output,unit,capacity,allocated,reserved,unknown,remaining:Math.max(0,capacity-allocated-reserved),overbooked:allocated+reserved>capacity,overlap};
 });
}
export function validateCargoCapacity(data,orders=[]){const ledger=cargoLedger(data,orders);for(const c of ledger){if(c.overbooked)throw Error('本组分配总量超过可用数量：'+c.capacity+c.unit+'，已分配/预约 '+(c.allocated+c.reserved)+c.unit);if(c.overlap)throw Error('本组各出库计划的箱唛子范围不能重复占用');}return ledger;}
