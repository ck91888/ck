// Explicit synthetic operator confirmation for authenticated business fixtures.
const contexts=new Map();
export const operationHeaders=cookie=>({'X-CK-Operation-Context':contexts.get(cookie)||''});
export function rememberOperation(result,cookie){if(result.user?.operation_context)contexts.set(cookie,result.user.operation_context.id);return result;}
export async function confirmOperation(call,name='隔离计划测试员'){
 const identity=await call('sop_identity');if(!identity.ok)throw Error(identity.error);
 const confirmed=await call('sop_operator_confirm',{name});if(!confirmed.ok)throw Error(confirmed.error);
 return confirmed;
}
