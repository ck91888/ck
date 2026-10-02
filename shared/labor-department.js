// Actual labor ownership is distinct from the cargo's business classification.
// Defaults follow the warehouse SOP; a confirmed dispatch assignment takes priority.
export const departments={bulk:'大货 / 대량',direct_ship:'代发 / 출고대행',import:'进口 / 수입'};
export const jobDefinitions={
 unload:['卸货','bulk'],unplanned_unload:['临时卸货','bulk'],
 inbound_direct:['代发入库','direct_ship'],inbound_bulk:['大货入库','bulk'],inbound_return:['退件入库','direct_ship'],
 inbound_change_order:['换单入库',''],pick_direct:['代发拣货','direct_ship'],bulk_op:['大货操作','bulk'],pack_direct:['代发打包 / 직배송 포장','direct_ship'],change_order:['换单操作','direct_ship'],
 load_outbound:['出库装货','bulk'],outbound_stock_op:['库内操作','bulk'],
 inventory:['盘点',''],disposal:['废弃处理',''],qc:['质检',''],issue_handle:['问题点处理',''],other_internal:['仓库整理',''],
 scan_pallet:['过机扫描','import'],load_import:['进口装柜','import'],pickup_delivery_import:['进口取送货','import'],verify_scan:['扫码核对','']
};
export const startJobTypes={v2_unload_job_start:'unload',v2_unplanned_unload_start:'unplanned_unload',v2_inbound_job_start:'',v2_import_delivery_job_start:'pickup_delivery_import',v2_outbound_load_start:'load_outbound',v2_outbound_stock_op_start:'outbound_stock_op',v2_issue_handle_start:'issue_handle',v2_pick_job_start:'pick_direct',v2_pick_job_start_by_docs:'pick_direct',v2_bulk_op_job_start:'bulk_op',v2_ops_job_start:'',v2_verify_job_start:'verify_scan'};
const recognized=value=>Object.hasOwn(departments,String(value||'').trim())?String(value).trim():'';
export function laborDepartment(job){
 const confirmed=recognized(job.labor_department)||(job.assignment_kind==='task'?recognized(job.assigned_department):'');
 if(confirmed)return confirmed;
 const definition=jobDefinitions[String(job.job_type??'').trim()];
 if(definition?.[1])return definition[1];
 const business=String(job.biz_class??'').trim();
 return recognized(business)||(business==='return'?'direct_ship':'other');
}
export function startDepartment(payload){return laborDepartment({...payload,job_type:payload.job_type||startJobTypes[payload.action]||''});}
