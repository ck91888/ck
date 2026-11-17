// Workflow reminders only: never create charges or modify ledger entries.
export async function issueAccounting(env,id){
 const row=await env.DB.prepare('SELECT accounting_required,accounting_required_by,accounting_required_at,accounting_note,accounted,accounted_by,accounted_at FROM v2_issue_tickets WHERE id=?').bind(id).first();
 if(!row)throw Error('问题不存在');
 return row;
}
