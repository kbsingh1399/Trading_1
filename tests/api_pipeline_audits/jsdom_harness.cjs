const {JSDOM}=require('jsdom');
const vm=require('vm');
const input=JSON.parse(require('fs').readFileSync(0,'utf8'));
function env(html){
  const dom=new JSDOM(html,{url:'https://www.coinglass.com/test/BTC',runScripts:'outside-only'});
  const w=dom.window;
  Object.defineProperty(w.HTMLElement.prototype,'innerText',{get(){return this.textContent;},set(v){this.textContent=v;},configurable:true});
  w.performance.getEntriesByType=()=>[];
  let clock=Date.now();
  const ctx=dom.getInternalVMContext();
  ctx.setTimeout=(fn)=>{clock+=200;fn();return 0;};
  ctx.Date=class extends Date { static now(){clock+=250;return clock;} };
  return {w,ctx,dom};
}
async function whale(mode){
  const {w,ctx}=env(`<body><div class="lo-rail"><div class="ui_select">Size threshold ≥ $0.5M</div></div><div class="lo-bt-body"><table><tbody></tbody></table></div><div id="zero">$0</div></body>`);
  const control=w.document.querySelector('.ui_select');
  let rows=[{ex:'bn-perp',side:'ask',price:100,usd:2e6,initUsd:2e6,startT:Date.now(),id:'big'}];
  let clicks=0;
  control.addEventListener('click',()=>{clicks++;});
  w.document.querySelector('#zero').addEventListener('click',()=>{
    control.textContent='Size threshold ≥ $0';
    rows.push({ex:'bn-perp',side:'bid',price:99,usd:250,initUsd:500,filledUsd:250,startT:Date.now(),id:'mid'});
    rows.push({ex:'bn-perp',side:'bid',price:98,usd:0,initUsd:0,startT:Date.now(),id:'zero'});
  });
  const body=w.document.querySelector('.lo-bt-body');
  if(mode!=='empty') body[Object.keys(body).find(k=>k.startsWith('__reactFiber$'))||'__reactFiber$audit']={memoizedState:{get memoizedState(){return {'0':rows};},next:null},return:null};
  if(mode==='dom'){
    delete body.__reactFiber$audit;
    body.querySelector('tbody').innerHTML='<tr><td>BINANCE Bid</td><td>$99.00</td><td>-1.00%</td><td>$1.2K</td><td>$750</td><td>$450</td><td>x</td><td>2m</td><td>Partially Filled</td></tr>';
  }
  const fn=vm.runInContext('('+input.code+')',ctx);
  const result=await fn('BTC');
  return {result,clicks};
}
const liqText=`Strength by distance
Total strength in each band from last price
0-1%
1.2M · 750.5K
By leverage tier
Open-level strength per leverage tier
High leverage
2.5M · 1.25M
Magnet zones
Where leveraged positions cluster
Shorts get liquidated
1
$101.5-102.5
12.5K
+1.50%
Width 0.98%
Longs get liquidated
1
$97.5-98.5
9.75K
-2.25%
Width 1.02%
Near-field balance
Open strength within ±3% of price
Longs 55.5% · 1.25M
Shorts 44.5% · 990.5K
Heavier on longs
`;
async function liquidation(mode){
  const {w,ctx}=env('<body></body>');
  for(const label of ['3D · 1m','7D · 3m','30D · 15m']){
    const b=w.document.createElement('button');b.textContent=label;b.setAttribute('aria-selected','false');
    b.addEventListener('click',()=>{if(mode!=='unverified') w.document.querySelectorAll('button').forEach(x=>x.setAttribute('aria-selected',String(x===b)));});
    w.document.body.appendChild(b);
  }
  const table=w.document.createElement('table');table.innerHTML='<tbody><tr><td>Short</td><td>$100.00</td><td>+1.5%</td><td>10x</td><td>$1.2M</td></tr><tr><td>Long</td><td>$99.50</td><td>−2.25%</td><td>25x</td><td>750.5K</td></tr><tr><td>Long</td><td>$90</td><td>-5%</td><td>5x</td><td>9B</td></tr></tbody>';w.document.body.appendChild(table);
  const text=w.document.createElement('pre');text.textContent=liqText;w.document.body.appendChild(text);
  if(mode==='empty') table.querySelector('tbody').innerHTML='';
  return {result:await vm.runInContext('('+input.code+')',ctx)};
}
async function footprint(mode){
  const {w,ctx}=env('<body></body>');
  const D={T:[1700000000000],O:[100],H:[104],L:[99],C:[103],F:[{b:[1,5,2,1],s:[0,0,0,0],bc:[1,5,2,1],sc:[0,0,0,0],lo:100,tick:1}],loading:mode==='stale',obs:{get:()=>null}};
  const term={prefs:{sym:'BTC',itv:'5m',bt:'time',tick:'auto',mkt:'agg-perp'},D,cache:{ana:{get:()=>({mx:5,mn:0})}},pickPair(){},setItv(v){this.prefs.itv=v;this.D={...D};},setBt(v){this.prefs.bt=v;this.D={...D};},setTick(v){this.prefs.tick=v;},sigSets(){return [];}};
  w.__cg_term=term;
  const fn=vm.runInContext('('+input.code+')',ctx);return {result:await fn('BTC'),itv:term.prefs.itv};
}
(async()=>{try{const out=input.kind==='whale'?await whale(input.mode):input.kind==='liq'?await liquidation(input.mode):await footprint(input.mode);process.stdout.write(JSON.stringify(out));}catch(e){console.error(e.stack);process.exit(2);}})();
