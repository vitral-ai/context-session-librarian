"""Real SQLite auto-rollback diagnostics; private fixtures, no Native endpoint."""
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid

from csl_bridge_store import Store, decode


class StoreRollbackTests(unittest.TestCase):
    def test_sqlite_full_preserves_original_error_and_prior_uncertainty(self):
        with tempfile.TemporaryDirectory() as directory, closing(Store(Path(directory) / 'bridge', create=True)) as store:
            request = {'version': 1, 'op': 'followup', 'id': str(uuid.uuid4()),
                       'source': 'sender', 'target': 'recipient', 'text': 'Synthetic work'}
            store.reserve('peer', request)
            store.operation(request['id'], 'uncertain', {'state': 'uncertain'})
            pages = store.db.execute('PRAGMA page_count').fetchone()[0]
            store.db.execute('PRAGMA max_page_count=' + str(pages))
            with self.assertRaises(sqlite3.OperationalError) as caught:
                with store.transaction() as db:
                    db.execute('UPDATE operations SET result=? WHERE id=?',
                               ('x' * (1024 * 1024), request['id']))
            self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_FULL)
            self.assertFalse(store.db.in_transaction)
            row = store.rows('SELECT state,result FROM operations WHERE id=?', (request['id'],))[0]
            self.assertEqual(row['state'], 'uncertain')
            self.assertEqual(decode(row['result']), {'state': 'uncertain'})
            with store.transaction() as db:
                self.assertEqual(db.execute('SELECT 1').fetchone()[0], 1)

    def test_exception_with_live_transaction_still_rolls_back(self):
        with tempfile.TemporaryDirectory() as directory, closing(Store(Path(directory) / 'bridge', create=True)) as store:
            with self.assertRaises(KeyboardInterrupt):
                with store.transaction() as db:
                    db.execute('INSERT INTO config VALUES(?,?)', ('synthetic', 'null'))
                    raise KeyboardInterrupt('Synthetic interruption')
            self.assertEqual(store.rows('SELECT * FROM config'), [])
            self.assertFalse(store.db.in_transaction)
