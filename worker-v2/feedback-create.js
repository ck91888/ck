import {feedbackLinkActor,feedbackSource,feedbackReceipt,feedbackLinkStatements,feedbackLinkDetail} from './feedback-link.js';
const q=(env,sql,...args)=>env.DB.prepare(sql).bind(...args);
const feedbackId=value=>{const id=typeof value==='string'?value.trim():'';if(!id||id.length>120)throw Error('缺少反馈编号 / 피드백 번호가 필요합니다');return id;};

export async function existingFeedbackPlan(env,id){
 const canonical=await q(env,'SELECT plan_id FROM ck_feedback_plan_links WHERE feedback_id=?',id).first();
 if(canonical){const linked=await feedbackLinkDetail(env,id);return {...linked,canonical:true};}
 const rows=(await q(env,`SELECT p.id AS plan_id,p.display_no,p.status,p.customer FROM v2_inbound_plans p
  WHERE p.source_feedback_id=? OR p.id=(SELECT inbound_plan_id FROM v2_field_feedbacks WHERE id=?) LIMIT 2`,id,id).all()).results||[];
 if(rows.length>1)throw Error('此反馈存在多份历史入库计划，请办公室核对 / 기존 입고계획을 확인하세요');
 return rows[0]?{...rows[0],feedback_id:id,canonical:false}:null;
}
export const feedbackExistingResult=link=>({ok:true,id:link.plan_id,inbound_plan_id:link.plan_id,display_no:link.display_no,feedback_id:link.feedback_id,job_id:link.job_id||'',already_linked:true,outbound_id:null,outbound_display_no:null,needs:[],outbounds:[]});

export async function feedbackCreatePreview(env,body){
 feedbackLinkActor(env);const id=feedbackId(body.feedback_id),existing=await existingFeedbackPlan(env,id);
 if(existing)return {ok:true,existing_link:existing,source:null};
 const src=await feedbackSource(env,id);
 return {ok:true,existing_link:null,source:{feedback_id:id,display_no:src.fb.display_no||id,feedback_version:src.fb.updated_at,job_id:src.job.id,job_version:src.job.updated_at,
  actual_totals:[...src.totals].map(([unit_type,actual_qty])=>({unit_type,actual_qty})),completed_at:src.fb.completed_at||src.job.updated_at,
  content:src.fb.content||'',remark:src.fb.remark||'',workers:src.workers.map(({worker_name,joined_at,left_at,minutes_worked})=>({worker_name,joined_at,left_at,minutes_worked}))}};
}
export async function prepareFeedbackCreate(env,context){
 const actor=feedbackLinkActor(env);
 if(!context||typeof context!=='object'||Array.isArray(context))throw Error('反馈来源无效，请重新打开新建表单');
 const id=feedbackId(context.feedback_id),existing=await existingFeedbackPlan(env,id);
 if(existing)return {existing};
 const src=await feedbackSource(env,id);
 if(context.feedback_version!==src.fb.updated_at||context.job_version!==src.job.updated_at)throw Error('原卸货记录已变化，请重新核对 / 하차 기록이 변경되었습니다');
 return {src,actor};
}
export function feedbackCreateStatements(env,prepared,plan,lines,time){
 if(lines.length>150)throw Error('明细过多，请先整理入库计划');
 if(lines.some(line=>!line.unit_type||!Number.isFinite(line.planned_qty)||line.planned_qty<0||line.planned_qty>1e8))throw Error('计划货物类型或数量无效，请核对明细');
 const mapped=feedbackReceipt(prepared.src,lines);
 if(mapped.some(line=>line.actual_qty===null))throw Error('来自现场反馈的新计划请将相同货物类型合并为一行，实际卸货数量不会改写 / 동일 유형을 한 행으로 합쳐 주세요');
 const actual=mapped.map(line=>({line_id:line.id||'IPL-'+crypto.randomUUID(),unit_type:line.unit_type,planned_qty:line.planned_qty,actual_qty:line.actual_qty,new_line:!line.id}));
 return feedbackLinkStatements(env,prepared.src,plan,lines,actual,prepared.actor,time);
}
