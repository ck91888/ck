// Synthetic read-only browser evidence fixture. No production credentials/data.
import assert from 'node:assert/strict';

export async function unifiedCheckFixture(DB,call,{volume=20001,shipDate='2026-11-01'}={}){
 assert.ok(volume>5000,'Fixture must exceed the old 5000-row detail cap');
 const large=volume-12;
 const definitions=[
  ['000-LARGE',large,'合成完整扫描 / 합성 완료'],
  ['000-SHORT',5,'合成不足 / 합성 부족'],
  ['000-OVER',2,'合成超扫 / 합성 초과'],
  ['000-NONE',3,'合成未扫 / 합성 미스캔'],
  ['000-RECHECK',4,'合成数量改版 / 합성 수량 변경'],
  ['000-REMOVED',1,'合成撤销 / 합성 취소'],
  ['000-MOVED',2,'合成改期移入 / 합성 날짜 이동'],
  ['000-DUP',1,'合成重复尝试 / 합성 중복 시도']
 ];
 const created=await call('sop_check_create',{ship_date:shipDate});
 await call('sop_check_items',{id:created.id,revision:1,items:definitions.map(([barcode,qty,customer])=>({barcode,qty,customer}))});
 const stored=DB.raw.prepare('SELECT * FROM sop_records WHERE id=?').get(created.id),state=JSON.parse(stored.state);
 const by=Object.fromEntries(state.items.map(item=>[item.barcode,item]));
 for(const item of state.items){item.pallets=[];item.recheck=false;item.removed=false;}
 by['000-LARGE'].scanned=large;
 by['000-SHORT'].scanned=2;
 by['000-OVER'].scanned=2;
 by['000-NONE'].scanned=0;
 Object.assign(by['000-RECHECK'],{scanned:0,recheck:true,previous_scanned:2,qty:4});
 Object.assign(by['000-REMOVED'],{scanned:1,removed:true,removal_reason:'合成客户撤销，保留历史证据',disposition:'合成留原托盘'});
 Object.assign(by['000-MOVED'],{scanned:0,recheck:true,moved_from:'CHECK-2026-10-31'});
 by['000-DUP'].scanned=1;
 state.exceptions=[
  {id:'SYNTHETIC-EX-OVER',barcode:'000-OVER',pallet:'SYNTHETIC-OVER-PALLET',type:'overflow',at:'2026-10-13T07:00:04Z',by:'合成扫描员',resolved:false},
  {id:'SYNTHETIC-EX-UNKNOWN-1',barcode:'000-OUTSIDE',pallet:'SYNTHETIC-OUTSIDE-PALLET',type:'not_found',at:'2026-10-13T07:00:08Z',by:'合成扫描员',resolved:false},
  {id:'SYNTHETIC-EX-UNKNOWN-2',barcode:'000-OUTSIDE',pallet:'SYNTHETIC-OUTSIDE-PALLET',type:'not_found',at:'2026-10-13T07:00:09Z',by:'合成扫描员',resolved:true,resolution:'合成留证据，已复核'}
 ];
 state.original_total=definitions.reduce((sum,[,qty])=>sum+qty,0)-2;
 state.adjusted_total=2;state.added_total=0;state.removed_total=1;
 state.rounds=[{slot:'11:00',at:'2026-10-13T02:00:00Z',by:'合成交接员',summary:{planned:state.original_total,scanned:0,pending:7,unresolved:0},note:'合成保留历史轮次，本脚本只读'}];
 DB.raw.prepare('UPDATE sop_records SET state=? WHERE id=?').run(JSON.stringify(state),created.id);
 DB.raw.prepare('DELETE FROM v2_verify_batch_items WHERE batch_id=? AND barcode=?').run(state.legacy_id||created.id,'000-REMOVED');
 const expectedLogs=[],insert=DB.raw.prepare('INSERT INTO v2_verify_scan_logs(id,batch_id,worker_id,worker_name,pallet_no,barcode,scan_result,message,scanned_at) VALUES(?,?,?,?,?,?,?,?,?)');
 const add=(barcode,pallet,result,at,n)=>{
  const log={id:'SYNTHETIC-CHECK-LOG-'+shipDate+'-'+String(n).padStart(6,'0'),batch_id:state.legacy_id||created.id,worker_id:'SYNTHETIC-SCANNER',worker_name:'合成扫描员',pallet_no:pallet,barcode,scan_result:result,message:'合成全量扫描证据，不代表实际仓库',scanned_at:at};
  insert.run(log.id,log.batch_id,log.worker_id,log.worker_name,log.pallet_no,log.barcode,log.scan_result,log.message,log.scanned_at);expectedLogs.push(log);
 };
 DB.raw.exec('BEGIN');
 try{
  const start=Date.parse('2026-10-01T00:00:00Z');
  for(let i=0;i<large;i++)add('000-LARGE',i===0?'SYNTHETIC-OLDEST-PALLET':'SYNTHETIC-P-'+String(i%241).padStart(3,'0'),'ok',new Date(start+i*1000).toISOString(),i);
  const rest=[
   ['000-SHORT','SYNTHETIC-SHORT-PALLET','ok'],['000-SHORT','SYNTHETIC-SHORT-PALLET','ok'],
   ['000-OVER','SYNTHETIC-OVER-PALLET','ok'],['000-OVER','SYNTHETIC-OVER-PALLET','ok'],['000-OVER','SYNTHETIC-OVER-PALLET','overflow'],
   ['000-RECHECK','SYNTHETIC-OLD-REV-PALLET','ok'],['000-RECHECK','SYNTHETIC-OLD-REV-PALLET','ok'],
   ['000-REMOVED','SYNTHETIC-REMOVED-PALLET','ok'],
   ['000-OUTSIDE','SYNTHETIC-OUTSIDE-PALLET','not_found'],['000-OUTSIDE','SYNTHETIC-OUTSIDE-PALLET','not_found'],
   ['000-DUP','SYNTHETIC-DUP-PALLET','ok'],['000-DUP','SYNTHETIC-DUP-PALLET','duplicate']
  ];
  rest.forEach(([barcode,pallet,result],i)=>add(barcode,pallet,result,new Date(Date.parse('2026-10-13T07:00:00Z')+i*1000).toISOString(),large+i));
  DB.raw.exec('COMMIT');
 }catch(error){DB.raw.exec('ROLLBACK');throw error;}
 assert.equal(expectedLogs.length,volume);
 const evidence={};
 for(const log of expectedLogs){
  const item=evidence[log.barcode]||(evidence[log.barcode]={barcode:log.barcode,total:0,ok:0,overflow:0,not_found:0,duplicate:0,last_at:'',pallets:new Set()});
  item.total++;if(Object.hasOwn(item,log.scan_result))item[log.scan_result]++;item.last_at=item.last_at>log.scanned_at?item.last_at:log.scanned_at;if(log.pallet_no)item.pallets.add(log.pallet_no);
 }
 for(const value of Object.values(evidence))value.pallets=[...value.pallets].sort();
 const palletEvidence={};
 for(const log of expectedLogs){const item=palletEvidence[log.pallet_no]||(palletEvidence[log.pallet_no]={pallet_no:log.pallet_no,total:0,ok:0,abnormal:0,last_at:''});item.total++;if(log.scan_result==='ok')item.ok++;else item.abnormal++;item.last_at=item.last_at>log.scanned_at?item.last_at:log.scanned_at;}
 const planned=state.items.filter(x=>!x.removed).reduce((sum,x)=>sum+x.qty,0);
 return{id:created.id,revision:stored.revision,state,volume,large,planned,evidence,palletEvidence,expectedLogs,filterCounts:{all:8,abnormal:6,completed:2,not_scanned:1,shortage:1,overflow:1,not_in_batch:1,recheck:2,diff:7,removed:1}};
}
