// Public outbound numbers are based on the planned ship date, not the internal OB id.
const pinyin = new Intl.Collator('zh-CN-u-co-pinyin');
const initials = [
  ['阿','A'],['八','B'],['擦','C'],['搭','D'],['峨','E'],['发','F'],
  ['嘎','G'],['哈','H'],['击','J'],['喀','K'],['拉','L'],['妈','M'],
  ['拿','N'],['哦','O'],['啪','P'],['期','Q'],['然','R'],['撒','S'],
  ['塌','T'],['哇','W'],['昔','X'],['压','Y'],['匝','Z']
];
const phraseInitials = new Map([
  ['重庆','CQ'],['厦门','XM'],['长安','CA'],['长春','CC'],
  ['长沙','CS'],['长城','CC'],['长江','CJ']
]);
function hanInitial(char){
  let letter='A';
  for(const [boundary,value] of initials){if(pinyin.compare(char,boundary)<0)break;letter=value;}
  return letter;
}
export function customerInitials(value){
  const name=String(value??'').normalize('NFKC').trim();
  let abbreviation='';
  for(let i=0;i<name.length&&abbreviation.length<12;){
    let phrase=false;
    for(const [word,letters] of phraseInitials){if(name.startsWith(word,i)){abbreviation+=letters;i+=word.length;phrase=true;break;}}
    if(phrase)continue;
    const rest=name.slice(i),latin=rest.match(/^[A-Za-z]+/);
    if(latin){abbreviation+=latin[0]===latin[0].toUpperCase()&&latin[0].length<=8?latin[0]:latin[0][0].toUpperCase();i+=latin[0].length;continue;}
    const code=rest.codePointAt(0),char=String.fromCodePoint(code);
    if((code>=0x3400&&code<=0x9fff)||(code>=0x20000&&code<=0x2fa1f))abbreviation+=hanInitial(char);
    i+=char.length;
  }
  return abbreviation.slice(0,12)||'KH';
}
export function outboundDisplayBase(customer,shipDate){
  const date=String(shipDate??'').slice(0,10);
  const parsed=new Date(date);
  if(!/^\d{4}-\d{2}-\d{2}$/.test(date)||Number.isNaN(parsed.getTime())||parsed.toISOString().slice(0,10)!==date)throw Error('预计出库日期无效');
  return 'CHU-'+customerInitials(customer)+'-'+date.replaceAll('-','');
}
export async function nextOutboundDisplayNo(env,customer,shipDate){
  const base=outboundDisplayBase(customer,shipDate);
  // Seed once from any existing public numbers, then allocate within one D1 transaction.
  const [, ,read]=await env.DB.batch([
    env.DB.prepare('INSERT OR IGNORE INTO ck_outbound_display_sequences(base,sequence) SELECT ?,COALESCE(MAX(CASE WHEN display_no=? THEN 1 ELSE CAST(substr(display_no,length(?)+2) AS INTEGER) END),0) FROM v2_outbound_orders WHERE display_no=? OR display_no GLOB ?').bind(base,base,base,base,base+'-[0-9]*'),
    env.DB.prepare('UPDATE ck_outbound_display_sequences SET sequence=sequence+1 WHERE base=?').bind(base),
    env.DB.prepare('SELECT sequence FROM ck_outbound_display_sequences WHERE base=?').bind(base)
  ]);
  const sequence=Number(read.results?.[0]?.sequence);
  if(!Number.isSafeInteger(sequence)||sequence<1)throw Error('出库单号生成失败');
  return sequence===1?base:base+'-'+String(sequence).padStart(2,'0');
}
