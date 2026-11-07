import {ownedHomeFixture} from './field-owned-home-fixture.mjs';
export async function officeDutyFixture(t){
 const f=await ownedHomeFixture();t?.after(()=>f.DB.raw.close());
 const employee=async(no='DUTY',department='order_processing',checkin=true)=>{const p=(await f.ok('office','sop_attendance_employee_register',{employeeNo:'SYNTH-'+no,name:'合成值班职员 '+no,department})).person;if(checkin)await f.ok('office','sop_attendance_checkin',{badge:p.badgeId});return p;};
 const p=await employee();
 const get=(person=p)=>f.ok('office','sop_office_duty_get',{badge:person.badgeId});
 const change=(kind,state,extra={})=>f.ok('office','sop_office_duty_'+kind,{badge:p.badgeId,...(kind==='start'?{}:{job_id:state.job.id,revision:state.job.revision}),...extra});
 return {...f,p,employee,get,change};
}
