// Creates fictional acceptance documents through the original handlers. Staging only.
export async function prepareDemo(env,call,sop){
 if(env.SOP_ENVIRONMENT!=='staging'||env.SOP_REQUEST_USER?.role!=='manager')return {ok:false,error:'仅测试环境经理可准备虚拟数据'};
 const date=new Date(Date.now()+9*3600000).toISOString().slice(0,10),who=env.SOP_REQUEST_USER.name;
 const step=async(action,data={})=>{const r=await call({action,client_req_id:'CK-DEMO-V2-'+date+'-'+action,...data});if(!r.ok)throw Error(action+': '+(r.error||r.message));return r;};
 const ib=await step('v2_inbound_plan_create',{plan_date:date,expected_arrival:date,customer:'虚拟验收客户',biz_class:'bulk',biz_classes:['bulk'],cargo_summary:'虚拟货物100箱',purpose:'operation_then_outbound',remark:'到货先贴标100箱，反馈成果后由客服安排出库',created_by:who,lines:[{unit_type:'box',planned_qty:100}]});
 const job=await step('v2_unload_job_start',{plan_id:ib.id,worker_id:'TEST-UNLOAD',worker_name:'虚拟卸货员',biz_class:'bulk'});
 await step('v2_unload_job_finish',{job_id:job.job_id,worker_id:'TEST-UNLOAD',complete_job:true,result_lines:[{unit_type:'box',actual_qty:100}],box_count:100,pallet_count:0,remark:'虚拟验收卸货完成'});
 const prior=await sop({action:'sop_linked',source_id:ib.id});
 let need=prior.items?.[0];if(!need){need=await sop({action:'sop_need_create',client_req_id:'CK-DEMO-NEED-'+date,department:'bulk',source_type:'inbound',source_id:ib.id,customer:'虚拟验收客户',title:'虚拟验收：100箱贴标',instructions:'100箱贴标；完成后反馈实际数量及位置，再由客服安排出库',owner:who,location:'内场-验收位'});if(!need.ok)throw Error(need.error);}
 const issue=await step('v2_issue_create',{biz_class:'bulk',customer:'虚拟验收客户',related_doc_no:ib.display_no,issue_description:'虚拟问题：核实标签并反馈',submitted_by:who});
 const check=await step('v2_verify_batch_upload',{batch_no:'验收-'+date,remark:'虚拟核对条码；可追加或撤销',created_by:who,rows:[{barcode:'TEST-BOX-001',customer_name:'虚拟验收客户',planned_box_count:2},{barcode:'TEST-BOX-002',customer_name:'虚拟验收客户',planned_box_count:1}]});
 return {ok:true,date,inbound_id:ib.id,need_id:need.id,issue_id:issue.id,batch_id:check.id,badges:['TEST-A|测试操作员甲','TEST-B|测试操作员乙']};
}
