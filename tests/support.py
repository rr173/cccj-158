"""Shared helpers for the test suite (stdlib unittest; no third-party deps)."""

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mrtx import Client


class TempRepo:
    """Fresh database with standard tables, cleaned up on exit."""

    def __enter__(self):
        self.dir = tempfile.mkdtemp(prefix="mrtx-test-")
        self.path = os.path.join(self.dir, "repo.db")
        self.client = Client(self.path)
        self.client.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                              ("active", "bool"), ("note", "null")])
        self.client.create_table("ledger", [("acct", "text"), ("amount", "int64"),
                                           ("active", "bool")])
        self.client.create_table("audit", [("tag", "text"), ("n", "int64"), ("ok", "bool")])
        return self

    def __exit__(self, *exc):
        shutil.rmtree(self.dir, ignore_errors=True)
        return False


def account(owner="x", balance=10, active=True, note=None):
    return {"owner": owner, "balance": balance, "active": active, "note": note}


def seed_accounts(client, rows):
    tx = client.begin()
    for rid, values in rows:
        tx.stage_insert("accounts", rid, values)
    return tx.commit()


def ids(rows):
    return [r["row_id"] for r in rows]
