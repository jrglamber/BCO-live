"""Project Exit Plan research-only Entry Lab.

This module has ZERO execution authority. It records point-in-time entry
challenger decisions and updates fixed-horizon outcomes from later raw signals.
It never places/blocks/closes/modifies broker trades.
"""
from __future__ import annotations
import json
from typing import Any, Dict, List, Optional

ENTRY_LAB_VERSION = "entry_lab_v2_outcome_repair_2026_10_10"
HORIZONS = (6, 12, 24, 48, 72, 96)


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "": return None
        return float(v)
    except Exception: return None


def _b(v: Any) -> Optional[bool]:
    if v is None: return None
    if isinstance(v, bool): return v
    s=str(v).strip().lower()
    if s in {"1","true","yes","y","on","bull","bullish","long"}: return True
    if s in {"0","false","no","n","off","bear","bearish","short"}: return False
    return None


def _rowdict(row: Any) -> Dict[str, Any]:
    try: return dict(row) if row is not None else {}
    except Exception: return {}


def _payload(raw_text: Any) -> Dict[str, Any]:
    try: x=json.loads(str(raw_text or "{}"))
    except Exception: return {}
    if not isinstance(x,dict): return {}
    return x.get("payload") if isinstance(x.get("payload"),dict) else x


def _ctx8(p):
    contexts=p.get("contexts")
    if isinstance(contexts,list):
        for c in contexts:
            if isinstance(c,dict) and str(c.get("context_tf") or c.get("tf") or "").upper()=="8H": return c
    return p


def ensure_schema(conn):
    pg=bool(getattr(conn,"postgres",False)); id_type="BIGSERIAL PRIMARY KEY" if pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
    conn.execute(f"""CREATE TABLE IF NOT EXISTS entry_lab_shadow (
      id {id_type}, created_at_utc TEXT NOT NULL, updated_at_utc TEXT NOT NULL,
      research_version TEXT NOT NULL, project TEXT NOT NULL, raw_signal_id BIGINT NOT NULL,
      asset TEXT NOT NULL, signal_time TEXT, challenger TEXT NOT NULL, decision INTEGER,
      decision_known INTEGER NOT NULL DEFAULT 1, decision_reason TEXT, production_candidate INTEGER,
      actual_entry INTEGER, entry_price REAL, sl_pct REAL, exec_tf TEXT, context_tf TEXT,
      return_1h_pct REAL, return_4h_pct REAL, return_8h_pct REAL, return_24h_pct REAL,
      efficiency_8h REAL, context_bull INTEGER, point_in_time_json TEXT,
      outcome_6h_r REAL, outcome_6h_mfe_r REAL, outcome_6h_mae_r REAL,
      outcome_12h_r REAL, outcome_12h_mfe_r REAL, outcome_12h_mae_r REAL,
      outcome_24h_r REAL, outcome_24h_mfe_r REAL, outcome_24h_mae_r REAL,
      outcome_48h_r REAL, outcome_48h_mfe_r REAL, outcome_48h_mae_r REAL,
      outcome_72h_r REAL, outcome_72h_mfe_r REAL, outcome_72h_mae_r REAL,
      outcome_96h_r REAL, outcome_96h_mfe_r REAL, outcome_96h_mae_r REAL,
      UNIQUE(raw_signal_id, challenger))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entry_lab_asset_challenger ON entry_lab_shadow(asset,challenger,raw_signal_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entry_lab_outcome48 ON entry_lab_shadow(challenger,outcome_48h_r)")


def _series(conn,asset,raw_signal_id,limit=25):
    rows=conn.execute("""SELECT id,pair,timestamp_readable,exec_close,exec_high,exec_low FROM raw_signals
      WHERE UPPER(pair)=UPPER(?) AND id<=? AND exec_close IS NOT NULL ORDER BY id DESC LIMIT ?""",(asset,int(raw_signal_id),int(limit))).fetchall()
    return list(reversed([_rowdict(r) for r in rows]))


def _ret(series,n):
    if len(series)<n+1:return None
    a=_f(series[-(n+1)].get("exec_close")); b=_f(series[-1].get("exec_close"))
    return None if a is None or b is None or a==0 else (b/a-1.0)*100.0


def _eff(series,n=8):
    if len(series)<n+1:return None
    xs=[_f(r.get("exec_close")) for r in series[-(n+1):]]
    if any(x is None for x in xs):return None
    vals=[float(x) for x in xs]; travel=sum(abs(vals[i]-vals[i-1]) for i in range(1,len(vals)))
    return 0.0 if travel<=0 else abs(vals[-1]-vals[0])/travel


def _known_all(*vals): return all(v is not None for v in vals)


def capture(conn,*,project,raw_signal_id,baseline_candidate,actual_entry,sl_pct,now_utc_iso):
    ensure_schema(conn); row=_rowdict(conn.execute("SELECT * FROM raw_signals WHERE id=? LIMIT 1",(int(raw_signal_id),)).fetchone())
    if not row:return {"ok":False,"reason":"raw_signal_missing","research_only":True}
    asset=str(row.get("pair") or "").upper(); p=_payload(row.get("raw_json")); c=_ctx8(p); series=_series(conn,asset,int(raw_signal_id),30)
    r1,r4,r8,r24=(_ret(series,n) for n in (1,4,8,24)); eff8=_eff(series,8)
    ctx_close20=_b(c.get("ctx_close_gt_ema20",c.get("close_gt_ema20"))); ctx_close50=_b(c.get("ctx_close_gt_ema50",c.get("close_gt_ema50")))
    ctx_hist=_b(c.get("ctx_hist_up",c.get("hist_up"))); ctx_rsi=_b(c.get("ctx_rsi_up",c.get("rsi_up"))); ctx_stack=_b(c.get("ctx_bull_stack",c.get("bull_stack")))
    exec_close20=_b(p.get("exec_close_gt_ema20")); exec_close50=_b(p.get("exec_close_gt_ema50")); exec_hist=_b(p.get("exec_hist_up")); exec_rsi=_b(p.get("exec_rsi_up"))
    strict_known=_known_all(ctx_close20,ctx_close50,ctx_hist,ctx_rsi,ctx_stack); momentum_known=_known_all(exec_close20,exec_close50,exec_hist,exec_rsi)
    challengers=[
      ("CONTROL_CURRENT",bool(baseline_candidate),True,"current production candidate"),
      ("STRICT_8H_TREND",bool(baseline_candidate and all([ctx_close20,ctx_close50,ctx_hist,ctx_rsi,ctx_stack])) if strict_known else None,strict_known,"control + confirmed 8H EMA20/EMA50/MACD/RSI/bull-stack"),
      ("MOMENTUM_CONFIRM",bool(baseline_candidate and all([exec_close20,exec_close50,exec_hist,exec_rsi])) if momentum_known else None,momentum_known,"control + execution-timeframe EMA20/EMA50/MACD/RSI confirmation"),
      ("CLEAN_TREND",bool(baseline_candidate and eff8 is not None and eff8>=0.40) if eff8 is not None else None,eff8 is not None,"control + trailing 8h directional efficiency >= 0.40"),
      ("PULLBACK_IN_TREND",bool(baseline_candidate and r8 is not None and r8>0 and eff8 is not None and eff8>=0.25 and r1 is not None and r1<=0) if None not in (r8,eff8,r1) else None,None not in (r8,eff8,r1),"control + positive 8h trend with non-positive latest 1h return"),
      ("MOMENTUM_4H",bool(baseline_candidate and r4 is not None and r4>0 and r1 is not None and r1>0 and eff8 is not None and eff8>=0.40) if None not in (r4,r1,eff8) else None,None not in (r4,r1,eff8),"control + positive 1h/4h momentum and clean 8h trend")]
    pit={"raw_features":{"ctx_close_gt_ema20":ctx_close20,"ctx_close_gt_ema50":ctx_close50,"ctx_hist_up":ctx_hist,"ctx_rsi_up":ctx_rsi,"ctx_bull_stack":ctx_stack,"exec_close_gt_ema20":exec_close20,"exec_close_gt_ema50":exec_close50,"exec_hist_up":exec_hist,"exec_rsi_up":exec_rsi},"derived_at_signal":{"return_1h_pct":r1,"return_4h_pct":r4,"return_8h_pct":r8,"return_24h_pct":r24,"efficiency_8h":eff8},"future_data_included":False}
    inserted=0
    for name,decision,known,reason in challengers:
        if conn.execute("SELECT id FROM entry_lab_shadow WHERE raw_signal_id=? AND challenger=? LIMIT 1",(int(raw_signal_id),name)).fetchone():continue
        conn.execute("""INSERT INTO entry_lab_shadow(created_at_utc,updated_at_utc,research_version,project,raw_signal_id,asset,signal_time,challenger,decision,decision_known,decision_reason,production_candidate,actual_entry,entry_price,sl_pct,exec_tf,context_tf,return_1h_pct,return_4h_pct,return_8h_pct,return_24h_pct,efficiency_8h,context_bull,point_in_time_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_utc_iso,now_utc_iso,ENTRY_LAB_VERSION,project,int(raw_signal_id),asset,str(row.get("timestamp_readable") or ""),name,(1 if decision else 0) if decision is not None else None,1 if known else 0,reason,1 if baseline_candidate else 0,(1 if actual_entry else 0) if actual_entry is not None else None,_f(row.get("exec_close")),float(sl_pct or 1.0),str(row.get("execution_tf") or p.get("execution_tf") or "1H"),str(row.get("context_tf") or p.get("context_tf") or "8H"),r1,r4,r8,r24,eff8,(1 if ctx_stack else 0) if ctx_stack is not None else None,json.dumps(pit,separators=(",",":"))))
        inserted+=1
    return {"ok":True,"inserted":inserted,"research_only":True,"execution_authority":False,"version":ENTRY_LAB_VERSION}


def update_outcomes(conn,asset,now_utc_iso,max_rows=1200):
    """Mature every known challenger decision, including rejected candidates.

    v1 filtered on decision=1, which made the lab unable to compare what a
    challenger accepted with what it rejected and could leave the exported
    outcome matrix effectively blank. This repair is research-only and does not
    touch production entry/exit state.
    """
    ensure_schema(conn)
    pending=conn.execute("""SELECT * FROM entry_lab_shadow
      WHERE UPPER(asset)=UPPER(?) AND decision_known=1 AND outcome_96h_r IS NULL
      ORDER BY raw_signal_id ASC LIMIT ?""",(asset,int(max_rows))).fetchall()
    updated=0; immature=0; invalid=0; horizons_written={str(h):0 for h in HORIZONS}
    for raw in pending:
        r=_rowdict(raw); rid=int(r.get("raw_signal_id") or 0); entry=_f(r.get("entry_price")); sl=_f(r.get("sl_pct"))
        if rid<=0 or entry is None or entry<=0 or sl is None or sl<=0:
            invalid+=1; continue
        future=[_rowdict(x) for x in conn.execute("""SELECT id,timestamp_readable,exec_close,exec_high,exec_low FROM raw_signals
          WHERE UPPER(pair)=UPPER(?) AND id>? AND exec_close IS NOT NULL ORDER BY id ASC LIMIT 96""",(asset,rid)).fetchall()]
        if len(future)<6: immature+=1; continue
        sets=[]; params=[]
        for h in HORIZONS:
            if len(future)<h: continue
            seg=future[:h]; last=_f(seg[-1].get("exec_close")); highs=[_f(x.get("exec_high")) for x in seg]; lows=[_f(x.get("exec_low")) for x in seg]
            highs=[x for x in highs if x is not None]; lows=[x for x in lows if x is not None]
            rr=((last/entry-1.0)*100.0/sl) if last is not None else None; mfe=((max(highs)/entry-1.0)*100.0/sl) if highs else None; mae=((min(lows)/entry-1.0)*100.0/sl) if lows else None
            sets += [f"outcome_{h}h_r=?",f"outcome_{h}h_mfe_r=?",f"outcome_{h}h_mae_r=?"]; params += [rr,mfe,mae]; horizons_written[str(h)]+=1
        if sets:
            sets.append("updated_at_utc=?"); params.extend([now_utc_iso,r.get("id")]); conn.execute("UPDATE entry_lab_shadow SET "+",".join(sets)+" WHERE id=?",tuple(params)); updated+=1
    return {"ok":True,"updated":updated,"pending_scanned":len(pending),"immature":immature,"invalid":invalid,"horizons_written":horizons_written,"research_only":True,"execution_authority":False,"version":ENTRY_LAB_VERSION}
