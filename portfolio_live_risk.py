"""Read-only broker-backed portfolio risk telemetry; no execution authority."""
from __future__ import annotations
import json, os, threading, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from typing import Any, Dict
VERSION="portfolio_live_risk_v1_2026_10_15"; TTL=20.0; _cache:Dict[str,Any]={}; _cache_at=0.0; _lock=threading.Lock()
def _now(): return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
def _f(v):
    try: return None if v is None or v=='' else float(v)
    except Exception: return None
def _base():
    x=os.getenv('OANDA_API_BASE','').strip()
    if x:return x.rstrip('/')
    return 'https://api-fxpractice.oanda.com' if os.getenv('OANDA_ENV','live').strip().lower() in {'practice','demo','sandbox'} else 'https://api-fxtrade.oanda.com'
def _get(base,account,token,path,params=None):
    q=urllib.parse.urlencode(params or {}); u=f"{base}/v3/accounts/{urllib.parse.quote(account,safe='')}/{path}"+(('?'+q) if q else '')
    r=urllib.request.Request(u,headers={'Authorization':f'Bearer {token}','Accept':'application/json'},method='GET')
    with urllib.request.urlopen(r,timeout=6.0) as h:return json.loads(h.read().decode())
def _allowed(ins):
    s=str(ins or '').upper(); cfg=os.getenv('BCO_OANDA_INSTRUMENT','').strip().upper()
    return s==cfg if cfg else ('BCO' in s or 'BRENT' in s)
def _mid(base,account,token,ins):
    try:
        p=(_get(base,account,token,'pricing',{'instruments':ins}).get('prices') or [])[0]; b=_f(p.get('closeoutBid')); a=_f(p.get('closeoutAsk'))
        return (b+a)/2 if b and a else None
    except Exception:return None
def _factor(base,account,token,q,h,memo):
    k=(q,h)
    if k in memo:return memo[k]
    if q==h:memo[k]=1.0;return 1.0
    d=_mid(base,account,token,f'{q}_{h}')
    if d and d>0:memo[k]=d;return d
    inv=_mid(base,account,token,f'{h}_{q}');memo[k]=(1/inv) if inv and inv>0 else None;return memo[k]
def _stop(t):
    for k in ('stopLossOrder','guaranteedStopLossOrder','trailingStopLossOrder'):
        o=t.get(k)
        if isinstance(o,dict):
            p=_f(o.get('price') or o.get('trailingStopValue'))
            if p and p>0:return p
    return None
def _calc():
    account=os.getenv('OANDA_ACCOUNT_ID','').strip();token=os.getenv('OANDA_API_TOKEN','').strip()
    if not account or not token:return {'ok':False,'version':VERSION,'scope':'bco','calculated_at_utc':_now(),'error':'OANDA credentials not configured','capital_at_risk_gbp':None}
    base=_base();home=str(((_get(base,account,token,'summary').get('account') or {}).get('currency') or 'GBP')).upper();trades=[t for t in (_get(base,account,token,'openTrades').get('trades') or []) if _allowed(t.get('instrument'))]
    memo={};total=floor=0.0;quant=no_stop=no_conv=0;by={}
    for t in trades:
        ins=str(t.get('instrument') or 'UNKNOWN').upper();entry=_f(t.get('price'));units=_f(t.get('currentUnits'));stop=_stop(t);row=by.setdefault(ins,{'open_trade_count':0,'quantified_trade_count':0,'without_stop_trade_count':0,'conversion_missing_trade_count':0,'capital_at_risk_gbp':0.0,'locked_profit_floor_gbp':0.0});row['open_trade_count']+=1
        if entry is None or units is None or abs(units)<=0 or stop is None:no_stop+=1;row['without_stop_trade_count']+=1;continue
        quote=ins.rsplit('_',1)[-1] if '_' in ins else home;fac=_factor(base,account,token,quote,home,memo)
        if fac is None:no_conv+=1;row['conversion_missing_trade_count']+=1;continue
        if units>0:loss=max(0.0,entry-stop);gain=max(0.0,stop-entry)
        else:loss=max(0.0,stop-entry);gain=max(0.0,entry-stop)
        risk=loss*abs(units)*fac;locked=gain*abs(units)*fac;total+=risk;floor+=locked;quant+=1;row['quantified_trade_count']+=1;row['capital_at_risk_gbp']+=risk;row['locked_profit_floor_gbp']+=locked
    for r in by.values():r['capital_at_risk_gbp']=round(r['capital_at_risk_gbp'],2);r['locked_profit_floor_gbp']=round(r['locked_profit_floor_gbp'],2)
    return {'ok':True,'version':VERSION,'scope':'bco','calculated_at_utc':_now(),'account_currency':home,'open_trade_count':len(trades),'quantified_trade_count':quant,'without_stop_trade_count':no_stop,'conversion_missing_trade_count':no_conv,'unquantified_trade_count':no_stop+no_conv,'capital_at_risk_gbp':round(total,2),'locked_profit_floor_gbp':round(floor,2),'by_instrument':by,'method':'broker_entry_to_current_stop_x_actual_units_converted_to_account_currency','execution_authority':False}
def _read():
    global _cache,_cache_at
    now=time.monotonic()
    with _lock:
        if _cache.get('ok') and now-_cache_at<TTL:o=dict(_cache);o['cached']=True;return o
    try:o=_calc()
    except Exception as e:
        with _lock:
            if _cache.get('ok'):o=dict(_cache);o.update({'cached':True,'stale':True,'error':f'{type(e).__name__}: {e}'});return o
        o={'ok':False,'version':VERSION,'scope':'bco','calculated_at_utc':_now(),'error':f'{type(e).__name__}: {e}','capital_at_risk_gbp':None}
    if o.get('ok'):
        with _lock:_cache=dict(o);_cache_at=now
    return o
def install(app:Any):
    try:app.add_api_route('/api/portfolio-risk',_read,methods=['GET'],include_in_schema=False)
    except Exception:pass
    return {'installed':True,'version':VERSION,'mode':'bco','execution_authority':False}
