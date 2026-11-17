// Read-only projection for the purchase picker. Caller keeps the original material-list authorization.
const fields='id,material_code,barcode,name_zh,name_ko,category,spec,unit,current_qty,CASE WHEN min_qty>0 AND current_qty<=min_qty THEN 1 ELSE 0 END AS low_stock';
const order='ORDER BY low_stock DESC,category,name_zh,material_code';
const searchable=['material_code','barcode','name_zh','name_ko','spec','category'];
export async function purchaseMaterialPage(DB,{search,search_locale,limit,offset}){
 let locale;try{if(search_locale)locale=Intl.getCanonicalLocales(String(search_locale))[0];}catch{throw Error('invalid_material_search_locale');}
 const term=String(search??'').trim().toLocaleLowerCase(locale);
 if(!term){
  const [count,page]=await DB.batch([
   DB.prepare("SELECT COUNT(*) AS c FROM v2_003_materials WHERE status='active'"),
   DB.prepare(`SELECT ${fields} FROM v2_003_materials WHERE status='active' ${order} LIMIT ? OFFSET ?`).bind(limit,offset)
  ]);
  return {items:page.results||[],total:count.results?.[0]?.c||0};
 }
 // One coherent read preserves category, literal %/_, Unicode and browser-locale
 // matching from the old local filter. Exclude notes, costs and unrelated metadata.
 const rows=(await DB.prepare(`SELECT ${fields} FROM v2_003_materials WHERE status='active' ${order}`).all()).results||[];
 const items=[];let total=0;
 for(const row of rows)if(searchable.some(key=>String(row[key]||'').toLocaleLowerCase(locale).includes(term))){
  if(total>=offset&&items.length<limit)items.push(row);total++;
 }
 return {items,total};
}
