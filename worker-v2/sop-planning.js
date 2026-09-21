// Work requirements and optional dispatch plans share the inbound transaction.
const str=v=>String(v??'').trim();
const required=(v,label)=>{const s=str(v);if(!s)throw Error(label+'不能为空');return s;};
const qty=v=>{const n=Number(v);if(!Number.isSafeInteger(n)||n<1)throw Error('计划数量必须为正整数');return n;};
export function workPlanStatements(env,rows,source,actor,t){
 if(!Array.isArray(rows)||rows.length>30)throw Error('每次最多建立30条作业需求');
 const statements=[],needs=[],outbounds=[];
 const sql=(s,...v)=>env.DB.prepare(s).bind(...v);
 for(const item of rows){
  const id=item.id||'NEED-'+crypto.randomUUID(),department=item.department||'bulk';
  if(!['bulk','direct_ship','import'].includes(department))throw Error('业务类型无效');
  const data={title:required(item.title,'作业名称'),customer:required(source.customer,'客户'),source_type:source.type,source_id:source.id||'',supply_chain_no:str(item.supply_chain_no||source.supply_chain_no),instructions:required(item.instructions,'客服文字要求'),scope_text:str(item.scope_text),owner:str(item.owner)||'工单处理员待接单',location:str(item.location),deadline:str(item.deadline),status:'pending',links:[],created_at:t,created_by:actor.name};
  if(source.type==='inventory')required(data.supply_chain_no,'供应链系统单号');
  if(item.planned_quantity){data.planned_quantity=qty(item.planned_quantity);data.planned_unit=required(item.planned_unit,'计划单位');}
  const obs=item.outbounds||[];if(!Array.isArray(obs)||obs.length>20)throw Error('每条作业最多关联20个出库计划');
  for(const ob of obs){
   if(!data.planned_quantity)throw Error('同步出库时请填写本作业的计划数量和单位');
   const quantity=qty(ob.quantity),unit=required(ob.unit||data.planned_unit,'出库数量单位');
   if(unit!==data.planned_unit)throw Error('出库分配单位须与作业计划单位一致');
   if(!['warehouse_dispatch','customer_pickup','milk_express','milk_pallet','container_pickup'].includes(ob.outbound_mode))throw Error('请选择出库方式');
   const ship=required(ob.expected_ship_at,'预计出库日期');if(!/^\d{4}-\d{2}-\d{2}$/.test(ship)||Number.isNaN(Date.parse(ship)))throw Error('出库日期无效');
   const obid='OB-'+crypto.randomUUID(),display='OB-'+ship.replaceAll('-','')+'-'+obid.slice(-8);
   data.links.push({outbound_id:obid,quantity,unit,phase:'planned',by:actor.name,at:t});outbounds.push({id:obid,display_no:display,need_id:id});
   statements.push(sql(`INSERT INTO v2_outbound_orders(id,order_date,customer,biz_class,outbound_mode,instruction,status,source_inbound_plan_id,created_by,created_at,updated_at,display_no,expected_ship_at,wms_work_order_no,planned_box_count,planned_pallet_count,uses_stock_operation,stock_operation_status,outbound_requirement,destination) VALUES(?,?,?,?,?,?,'pending_issue',?,?,?,?,?,?,?,?,?,0,'pending',?,?)`,obid,new Date(Date.parse(t)+9*3600000).toISOString().slice(0,10),data.customer,department==='import'?'bulk':department,ob.outbound_mode,data.instructions,source.type==='inbound'?source.id:'',actor.name,t,t,display,ship,data.supply_chain_no,unit==='箱'?quantity:0,unit==='托'?quantity:0,str(ob.outbound_requirement),str(ob.destination)));
  }
  if(data.links.reduce((n,x)=>n+x.quantity,0)>(data.planned_quantity||0))throw Error('出库分配数量超过本作业计划数量');
  const state=JSON.stringify(data);
  statements.push(sql('INSERT INTO sop_events VALUES(?,?,?,?,?,?,?,?,?,?)','CREATE-'+id,id,0,'sop_need_create',actor.id,actor.name,'{}',state,JSON.stringify({ok:true,id,revision:1}),t));
  statements.push(sql('INSERT INTO sop_records VALUES(?,?,?,?,?,?)',id,'need',1,department,state,t));needs.push({id,...data});
 }
 return {statements,needs,outbounds};
}
