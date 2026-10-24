import {canonicalDailyBadge} from '../shared/badge-identity.js';

// A committed request may predate canonical daily badge spelling. Compare only
// identity fields through the alias rule; preserve every other request value.
export function sameBadgeSignature(left,right,{sortedWorkers=false}={}){
 if(left===right)return true;
 const normalize=value=>{
  const data=JSON.parse(value);
  function visit(node){
   if(!node||typeof node!=='object')return;
   for(const key of ['worker_id','handler_id','lead_id'])if(typeof node[key]==='string')node[key]=canonicalDailyBadge(node[key]);
   for(const key of ['workers','assigned_workers'])if(Array.isArray(node[key])){
    node[key]=node[key].map(w=>({...w,id:canonicalDailyBadge(w.id)}));
    if(sortedWorkers)node[key].sort((a,b)=>a.id.localeCompare(b.id));
   }
   if(typeof node.borrow_signature==='string')node.borrow_signature=normalize(node.borrow_signature);
   for(const child of Object.values(node))if(Array.isArray(child))child.forEach(visit);else visit(child);
   if(sortedWorkers&&Array.isArray(node.confirmations))node.confirmations.sort((a,b)=>a.worker_id.localeCompare(b.worker_id));
  }
  visit(data);return JSON.stringify(data);
 };
 try{return normalize(left)===normalize(right);}catch{return false;}
}
