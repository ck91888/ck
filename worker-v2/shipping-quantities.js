// Shipping packages are explicit facts, never inferred from work/consumable metrics.
const aliases = new Map([
 ['箱',['箱','纸箱','box','boxes','carton','cartons']],
 ['托',['托','托盘','pallet','pallets','plt','plts']],
 ['件',['件','piece','pieces','pcs']],['单',['单']],['批',['批']]
].flatMap(([unit,values])=>values.map(value=>[value,unit])));
export const shippingUnit = value => {const s=String(value??'').trim();return aliases.get(s.toLowerCase())||s;};
export function shippingComponents(values) {
 if(!Array.isArray(values)||!values.length||values.length>10)throw Error('请填写1至10项实际出货构成');
 const totals=new Map();
 for(const value of values){
  const unit=shippingUnit(value.unit),quantity=Number(value.quantity);
  if(!['箱','托','件','单','批'].includes(unit))throw Error('出货单位无效');
  if(!Number.isSafeInteger(quantity)||quantity<1)throw Error('各单位数量必须为正整数；不再出货的单位请移除');
  const total=(totals.get(unit)||0)+quantity;if(!Number.isSafeInteger(total))throw Error('数量超出范围');totals.set(unit,total);
 }
 return [...totals].map(([unit,quantity])=>({quantity,unit}));
}
export function shippingCapacity(data,links) {
 const basis=data.result||data.shipping_basis;
 const components=data.shipping_output?.components||data.result?.shipping_components||(!data.result&&data.shipping_basis?.components)||[{quantity:Number(basis?.quantity??data.planned_quantity)||0,unit:shippingUnit(basis?.unit||data.planned_unit)}];
 const units=new Map(components.map(c=>[shippingUnit(c.unit),{unit:shippingUnit(c.unit),quantity:Number(c.quantity),used:0}]));
 for(const link of links){const unit=shippingUnit(link.unit);if(!units.has(unit))units.set(unit,{unit,quantity:0,used:0});units.get(unit).used+=Number(link.quantity);}
 const by_unit=[...units.values()].map(c=>({...c,remaining:Math.max(0,c.quantity-c.used)}));
 const single=by_unit.length===1?by_unit[0]:null;
 return {by_unit,quantity:single?.quantity??null,unit:single?.unit||'',used:single?.used??null};
}
export function reserveShipping(capacity,rows) {
 const used=new Map();
 return rows.map(row=>{
  const unit=shippingUnit(row.unit|| (capacity.by_unit.length===1?capacity.by_unit[0].unit:'')),quantity=Number(row.quantity);
  if(!Number.isSafeInteger(quantity)||quantity<1)throw Error('数量必须为正整数');
  const available=capacity.by_unit.find(c=>c.unit===unit);if(!available)throw Error('请选择已审核出货构成中的单位');
  used.set(unit,(used.get(unit)||0)+quantity);
  if(!Number.isSafeInteger(used.get(unit))||used.get(unit)>available.remaining)throw Error('本次出库数量超过该单位剩余可安排数量：'+unit);
  return {...row,quantity,unit};
 });
}
export const shippingFullyAllocated=(capacity,rows=[])=>capacity.by_unit.every(c=>c.remaining===rows.filter(r=>r.unit===c.unit).reduce((n,r)=>n+r.quantity,0));
export function assertShippingCapacity(components,links) {
 const a=shippingCapacity({shipping_output:{components}},links);
 if(a.by_unit.some(c=>c.used>c.quantity))throw Error('实际出货构成不能少于各单位已占用数量（含已发货）；请核对已有出库');
}

// Source ordinals identify cartons within this work requirement, never a conversion ratio.
export function expectedShippingSources(data, values){
 if(shippingUnit(data.planned_unit)!=='箱'||!Number.isSafeInteger(data.planned_quantity)||data.planned_quantity<1)throw Error('请先明确本作业来源箱数；其他来源单位沿用原流程');
 if(!Array.isArray(values)||!values.length||values.length>20)throw Error('请填写1至20段来源箱范围及预计包装');
 const sources=values.map(v=>{const start=Number(v.start),end=Number(v.end);if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<1||end<start||end>data.planned_quantity)throw Error('来源箱序范围无效或超出本作业');const components=shippingComponents(v.components);if(components.some(c=>!['箱','托'].includes(c.unit)))throw Error('预计包装请填写箱或托');return {start,end,quantity:end-start+1,components};}).sort((a,b)=>a.start-b.start);
 let next=1;for(const source of sources){if(source.start!==next)throw Error('来源箱范围必须完整覆盖本作业，不能重复占用或遗漏；余散箱请单列');next=source.end+1;}
 if(next!==data.planned_quantity+1)throw Error('来源箱范围必须完整覆盖本作业；余散箱请单列');
 const totals=new Map();for(const c of sources.flatMap(s=>s.components))totals.set(c.unit,(totals.get(c.unit)||0)+c.quantity);
 return {sources,components:shippingComponents([...totals].map(([unit,quantity])=>({unit,quantity})))};
}
export function resultShippingComponents(result){
 if(result.shipping_components)return shippingComponents(result.shipping_components);
 const keys=[['shipping_box_count','箱'],['shipping_pallet_count','托']];
 if(!keys.some(([key])=>result[key]!==undefined&&result[key]!==''))return null;
 const components=[];for(const [key,unit] of keys){const quantity=Number(result[key]||0);if(!Number.isSafeInteger(quantity)||quantity<0)throw Error('实际独立出货包装须为非负整数');if(quantity)components.push({quantity,unit});}
 return shippingComponents(components);
}
export function shippingReady(data,links){
 if(!data.result||data.status==='cancelled')return false;
 return shippingCapacity(data,links).by_unit.every(c=>Number.isSafeInteger(c.used)&&c.used>=0&&c.used<=c.quantity);
}
