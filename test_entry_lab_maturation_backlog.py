import sqlite3
from datetime import datetime,timedelta,timezone
import unittest
from entry_lab import ensure_schema, update_outcomes

class EntryLabMaturationTests(unittest.TestCase):
    def test_invalid_rows_do_not_block_valid_outcomes(self):
        db=sqlite3.connect(':memory:')
        db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE raw_signals (id INTEGER PRIMARY KEY, pair TEXT, timestamp_readable TEXT, exec_close REAL, exec_high REAL, exec_low REAL)')
        ensure_schema(db)
        cols='created_at_utc,updated_at_utc,research_version,project,raw_signal_id,asset,challenger,decision_known,entry_price,sl_pct'
        for i in range(1205):
            db.execute(f'INSERT INTO entry_lab_shadow ({cols}) VALUES (?,?,?,?,?,?,?,?,?,?)',
                       ('2026-10-01','2026-10-01','test','bco',i+1,'BCO','shadow',1,None,None))
        db.execute(f'INSERT INTO entry_lab_shadow ({cols}) VALUES (?,?,?,?,?,?,?,?,?,?)',
                   ('2026-10-01','2026-10-01','test','bco',1300,'BCO','shadow',1,100.,1.))
        start=datetime(2026,10,1,tzinfo=timezone.utc)
        db.execute('INSERT INTO raw_signals VALUES (?,?,?,?,?,?)',
                   (1300,'BCO',start.isoformat(),100.,100.,100.))
        for h in range(1,100):
            db.execute('INSERT INTO raw_signals VALUES (?,?,?,?,?,?)',
                       (1300+h,'BCO',(start+timedelta(hours=h)).isoformat(),101.,102.,100.))
        result=update_outcomes(db,'BCO','2026-11-07',max_rows=1200)
        self.assertEqual(result['invalid_pending'],1205)
        self.assertEqual(result['updated'],1)
        self.assertEqual(result['horizons_written']['96'],1)
        self.assertEqual(result['pending_unscanned'],1205)

    def test_missing_exact_six_hour_candle_is_not_mislabeled(self):
        db=sqlite3.connect(':memory:'); db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE raw_signals (id INTEGER PRIMARY KEY, pair TEXT, timestamp_readable TEXT, exec_close REAL, exec_high REAL, exec_low REAL)')
        ensure_schema(db)
        start=datetime(2026,10,1,tzinfo=timezone.utc)
        db.execute('INSERT INTO raw_signals VALUES (?,?,?,?,?,?)',(1,'BCO',start.isoformat(),100.,100.,100.))
        db.execute('INSERT INTO entry_lab_shadow (created_at_utc,updated_at_utc,research_version,project,raw_signal_id,asset,signal_time,challenger,decision_known,entry_price,sl_pct) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                   ('2026-10-01','2026-10-01','test','bco',1,'BCO',start.isoformat(),'control',1,100.,1.))
        for hour in (1,2,3,4,5,7,8,9,10,11,12):
            db.execute('INSERT INTO raw_signals VALUES (?,?,?,?,?,?)',
                       (hour+1,'BCO',(start+timedelta(hours=hour)).isoformat(),101.,102.,99.))
        result=update_outcomes(db,'BCO','2026-11-07')
        row=db.execute('SELECT outcome_6h_r,outcome_12h_r FROM entry_lab_shadow').fetchone()
        self.assertIsNone(row['outcome_6h_r'])
        self.assertIsNotNone(row['outcome_12h_r'])
        self.assertEqual(result['horizon_basis'],'exact_elapsed_utc_hours')

if __name__=='__main__': unittest.main()
