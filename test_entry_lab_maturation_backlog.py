import sqlite3
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
        for i in range(1301,1400):
            db.execute('INSERT INTO raw_signals VALUES (?,?,?,?,?,?)',(i,'BCO','2026-10-01',101.,102.,100.))
        result=update_outcomes(db,'BCO','2026-11-07',max_rows=1200)
        self.assertEqual(result['invalid_pending'],1205)
        self.assertEqual(result['updated'],1)
        self.assertEqual(result['horizons_written']['96'],1)
        self.assertEqual(result['pending_unscanned'],1205)

if __name__=='__main__': unittest.main()
