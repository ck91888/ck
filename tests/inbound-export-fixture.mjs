// Persisted quantity fixtures only; never seed real/staging business data.
export const quantityColumns=['计划箱数','实际箱数','计划托数','实际托数'];
export const quantityCases=[
 {id:'courier-eight',lines:[['courier',8,8,0]],expected:[8,8,0,0]},
 {id:'mixed',lines:[['courier',8,6,8],['carton',5,4,3],['pallet',2,1,0]],expected:[13,9,2,1]},
 {id:'historical-lines',lines:[['courier',3,2,null],['courier',5,4,0]],expected:[8,6,0,0]},
 {id:'missing-actual',lines:[['courier',8,null,8]],expected:[8,0,0,0]},
 {id:'zero-actual',lines:[['courier',8,0,8]],expected:[8,0,0,0]},
 {id:'zero-plan',lines:[['courier',0,2,2]],expected:[0,2,0,0]},
 {id:'zero-all',lines:[['courier',0,0,0]],expected:[0,0,0,0]},
 {id:'old-units',lines:[['carton',12,10,9],['carton',3,2,1],['pallet',4,3,2],['cbm',99,88,77],['container_large',2,1,0],['container_small',1,1,0]],expected:[15,10,4,2]},
 {id:'old-fallback',lines:[['carton',12,10,0],['pallet',4,3,null]],expected:[12,10,4,3]},
 {id:'fractional-old',lines:[['carton',1.5,1.25,0],['pallet',2.5,1.5,1.25],['courier',8,6,0]],expected:[9.5,7.25,2.5,1.25]},
 {id:'no-lines',lines:[],expected:[0,0,0,0]}
];
export function seedQuantityPlan(DB,spec,index=0){
 const id='QA-EXPORT-'+spec.id;
 DB.raw.prepare(`INSERT INTO v2_inbound_plans(id,display_no,customer,cargo_summary,biz_class,biz_classes_json,plan_date,expected_arrival,created_at,source_type,status)
 VALUES(?,?,?,?,'bulk','["bulk"]','2026-10-01',?,'2026-10-01T00:00:00Z','manual','pending')`).run(id,id,'QA 合成导出客户','合成货物 "箱", 数量以明细为准\n不从文字推算999箱','2026-10-'+String(1+index%28).padStart(2,'0'));
 for(const [i,[unit,planned,actual,putaway]] of spec.lines.entries())DB.raw.prepare('INSERT INTO v2_inbound_plan_lines(id,plan_id,line_no,unit_type,planned_qty,actual_qty,putaway_qty) VALUES(?,?,?,?,?,?,?)').run(id+'-L'+i,id,i+1,unit,planned,actual,putaway);
 return id;
}
export const quantityValues=row=>quantityColumns.map(k=>row[k]);
