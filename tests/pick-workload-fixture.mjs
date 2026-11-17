// Synthetic start-time plan, only for actual picking entry actions.
export function pickStartFixture(action){return ['v2_pick_job_start','v2_pick_job_start_by_docs'].includes(action)?{planned_pick_workload:{order_count:23,product_quantity:120,sku_count:8,location_count:6,forklift:false}}:{};}
// Construct pre-rollout work using the existing internal invocation flag.
// This flag belongs to the test env, never the request body. Authentication,
// attendance and badge normalization still run. Restore it before every assertion.
export async function historicalPickStart(env,invoke,body){const native=env.SOP_NATIVE_START;env.SOP_NATIVE_START=true;try{return await invoke('v2_pick_job_start_by_docs',body);}finally{if(native===undefined)delete env.SOP_NATIVE_START;else env.SOP_NATIVE_START=native;}}
// Fill only the picking form under test; other operation forms stay untouched.
export async function fillPickWorkloadFixture(form){for(const [key,value] of [['order_count','23'],['product_quantity','120'],['sku_count','8'],['location_count','6']])await form.locator('[name=pick_'+key+']').fill(value);await form.locator('[name=pick_forklift][value=no]').check();}
