"""mrtx: a managed-repository transaction layer over SQLite.

Public surface::

    from mrtx import Client, eq, lt, le, is_null, and_, or_, not_

    c = Client("data.db")
    c.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                ("active", "bool"), ("note", "null")])
    with c.begin() as tx:
        rows = tx.read("accounts", where=eq("active", True))
        tx.stage_update("accounts", rows[0]["row_id"], {"active": False},
                        expected_version=rows[0]["version"])
    # commit happens on context exit

See ``docs/DESIGN.md`` for the isolation and conflict rules.
"""

from .engine import Client, Transaction, TransactionHandle, new_txid
from .errors import (ConflictError, ManagedTxError, PredicateError, SchemaError,
                     TransactionError, TransactionNotFound, BusyError)
from .values import (And, Atom, FALSE, LiteralFalse, LiteralTrue, Not, Or, TRUE,
                     and_, eq, is_null, le, lt, not_, or_)

__all__ = [
    "Client", "Transaction", "TransactionHandle", "new_txid",
    "ConflictError", "ManagedTxError", "PredicateError", "SchemaError",
    "TransactionError", "TransactionNotFound", "BusyError",
    "Atom", "And", "Or", "Not", "TRUE", "FALSE", "LiteralTrue", "LiteralFalse",
    "eq", "lt", "le", "is_null", "and_", "or_", "not_",
]
