// Corrections replace their referenced result, while independent outputs remain additive.
export function effectiveResults(rows) {
 const replaced=new Set();
 for(const row of rows){if(row.source==='manual_correction'&&row.previous_result_id&&rows.some(old=>old.id===row.previous_result_id&&old.job_id===row.job_id&&old.id!==row.id))replaced.add(row.previous_result_id);}
 return rows.filter(row=>!replaced.has(row.id));
}
