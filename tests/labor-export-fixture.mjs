import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
import {confirmOperation} from './office-operator-fixture.mjs';
const NativeDate=Date;
export async function atTime(start,fn){let time=NativeDate.parse(start);globalThis.Date=class extends NativeDate{constructor(...a){super(...(a.length?a:[time]));}static now(){return time;}};try{return await fn(iso=>{time=NativeDate.parse(iso);});}finally{globalThis.Date=NativeDate;}}
export async function office(f){await f.ok('office','sop_login',{sop_key:'synthetic-owned-home-fixture'});await confirmOperation((a,b)=>f.ok('office',a,b),'合成工时核验员');}

export async function exportFixture(){const f=await ownedHomeFixture();f.env.SOP_ATTENDANCE_RETURNS_ENABLED='true';await f.ok('office','sop_attendance_summary',{date:'2026-01-02'});return f;}
export function seedDaily(f,index,date='2026-01-02',{name,agency='가온',out='18:00:00',start='09:00:00'}={}){const id='FIN-P-'+index,badge='DAF-FIN-'+String(index).padStart(6,'0'),dayId='FIN-D-'+date+'-'+index,at=date+'T'+start+'+09:00';if(!f.DB.raw.prepare('SELECT id FROM ck_attendance_people WHERE id=?').get(id))f.DB.raw.prepare('INSERT INTO ck_attendance_people VALUES(?,?,?,?,?,?,?)').run(id,badge,name||'合成财务人员'+String(index).padStart(4,'0'),agency,'permanent',1,at);f.DB.raw.prepare('INSERT INTO ck_attendance_days VALUES(?,?,?,?,?,?,?,?,?,?)').run(dayId,id,badge,name||'合成财务人员'+String(index).padStart(4,'0'),agency,date,'synthetic-'+index,at,out?date+'T'+out+'+09:00':'',1);return {id,badge,dayId};}
