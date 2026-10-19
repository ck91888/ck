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
 const components=data.shipping_output?.components||[{quantity:Number(basis?.quantity??data.planned_quantity)||0,unit:shippingUnit(basis?.unit||data.planned_unit)}];
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
