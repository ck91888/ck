import {ensureResetSchema} from './test-data-reset.js';

const messages=error=>{
 const parts=[],seen=new Set();
 for(let current=error;current&&!seen.has(current)&&parts.length<4;current=current.cause){
  seen.add(current);parts.push(String(current.message||''));
 }
 return parts.join(' ');
};
const missingTable=error=>/no such table:\s*(?:main\.)?["'`]?ck_test_resets(?:["'`]|\b)/i.test(messages(error));
const read=env=>env.DB.prepare("SELECT 1 AS active FROM ck_test_resets WHERE completed_at='' LIMIT 1").first();

// A normal business request needs a fresh existence check, not maintenance DDL.
// Never cache the result: an active/interrupted run must block the next request.
export async function maintenanceInProgress(env){
 try{return !!await read(env);}
 catch(error){
  if(!missingTable(error))throw error;
  // Preserve fresh-database bootstrap. Failure to initialize or reread is closed.
  await ensureResetSchema(env);
  return !!await read(env);
 }
}

// Return only an allowlisted classification. D1 messages/SQL/bindings can contain
// business values and must not be copied to a public response or console output.
export function maintenanceFailure(error){
 const message=messages(error);
 const code=/SQLITE_FULL|database.*(?:disk is full|maximum size)|storage.*(?:limit|quota|full)|disk.*full/i.test(message)?'M-STORAGE':
  /(?:daily|today|per.day).*(?:limit|maximum|exceed)|(?:limit|maximum|exceed).*(?:daily|today|per.day)/i.test(message)?'M-QUOTA':
  /overload|too many (?:requests|queries)|rate.?limit/i.test(message)?'M-BUSY':
  /time.?out|timed out/i.test(message)?'M-TIMEOUT':
  /no such (?:table|column)|SQLITE_SCHEMA|schema.*(?:changed|invalid)/i.test(message)?'M-SCHEMA':'M-DATABASE';
 return {ok:false,code,error:'测试维护状态读取失败，请稍后重试 ['+code+'] / 잠시 후 다시 시도하세요'};
}
