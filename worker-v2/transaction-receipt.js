// Store a fingerprint rather than another raw copy of the request content.
export function canonicalIntent(value){
 const normalize=x=>Array.isArray(x)?x.map(normalize):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,normalize(x[k])])):x;
 return JSON.stringify(normalize(value));
}
export async function intentSignature(value){
 return [...new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(canonicalIntent(value))))].map(b=>b.toString(16).padStart(2,'0')).join('');
}
export const receiptConflict=()=>new Error('request_id_conflict');
export const boundReceiptJSON=(response,signature)=>JSON.stringify({...response,_request_signature:signature});
export async function readBoundReceipt(env,{key,action,signature,legacyMatch,load}){
 if(!key)return null;
 const row=await (load?load():env.DB.prepare('SELECT action,response_json FROM v2_idempotency_keys WHERE idem_key=?').bind(key).first());
 if(!row)return null;
 if(row.action!==action)throw receiptConflict();
 let saved;try{saved=JSON.parse(row.response_json);}catch{throw receiptConflict();}
 if(!saved||saved.ok!==true)throw receiptConflict();
 const {_request_signature:previous,...response}=saved;
 if(Object.hasOwn(saved,'_request_signature')){if(previous!==signature)throw receiptConflict();}
 else if(!legacyMatch||!await legacyMatch(response))throw receiptConflict();
 return response;
}
