import {inboundCode,inboundFlowEnabled,resolveInboundPlan} from './inbound-flow.js';
import {stagedInbound,tallyType} from './inbound-stages.js';
import {documentCodeError} from '../shared/document-code.js';

export const independentSource='direct_tally_session';
export const independentInbound=plan=>plan?.source_type===independentSource;
// Reuse the existing external-inbound customer text, without creating master data.
export function inboundOwner(value){
 const name=typeof value==='string'?value.trim():'';
 if(!name)throw Error('请填写货主 / 화주를 입력하세요');
 if(name.length>120||/[\u0000-\u001f\u007f]/.test(name))throw Error('货主最多120字符，不能包含控制字符 / 화주는 120자 이내로 입력하세요');
 return name;
}

// Recognition is read-only. Only an authorized atomic dispatch creates a session.
// The shared plan storage supplies existing job/clock/detail relationships; these
// sessions have no RU number, unload evidence or planned stock.
export async function resolveInboundEntry(env,value,biz,type){
 const code=inboundCode(value),blocked=message=>({ok:true,kind:'status_not_allowed',message});
 if(!inboundFlowEnabled(env)||!stagedInbound(type))return resolveInboundPlan(env,code,biz);
 if(biz!=='direct_ship')return blocked('请从代发理货或上架入口操作');
 const invalid=documentCodeError(code);
 if(invalid||code.length>120||/[,;，；\t\r\n]/.test(code))return blocked(invalid||'请填写一个有效入库单号（最多120字符）');
 // Include cancelled/deleted documents in the probe: a typo or retired reference
 // must never silently become an unrelated new session.
 const known=(await env.DB.prepare(`SELECT id,source_type,status,is_deleted FROM v2_inbound_plans
  WHERE instr(char(10)||COALESCE(external_inbound_no,'')||char(10),char(10)||?||char(10))>0 OR display_no=? OR id=?
  ORDER BY (status!='cancelled' AND COALESCE(is_deleted,0)=0 AND source_type!='return_session') DESC LIMIT 2`).bind(code,code,code).all()).results||[];
 if(!known.length){
  if(type!==tallyType)return blocked('本单理货尚未完成，不能开始上架 / 검수 완료 후 적치를 시작하세요');
  if(/^(?:RU-|IB-|ITS-|CK[A-Z]+\|)/i.test(code))return blocked('未找到此系统单号，请核对原单 / 시스템 번호를 확인하세요');
  return {ok:true,kind:'independent',code,plan:null};
 }
 // Old cancelled references may already have been legitimately reused by an
 // existing active plan. Preserve that association; only prohibit fresh sessions.
 if(known[0].status==='cancelled'||known[0].is_deleted)return blocked('此单号已取消或删除，不能重新开工 / 취소된 입고번호입니다');
 const result=await resolveInboundPlan(env,code,biz);
 if(result.kind==='system'&&independentInbound(result.plan))result.kind='independent';
 return result;
}
