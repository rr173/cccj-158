"""Independent serializability checker for small committed histories.

This module is deliberately *outside* the product.  It imports nothing from
``mrtx``: it opens the SQLite file directly with ``sqlite3``, decodes the
persisted evidence, implements its own three-valued predicate evaluator, and
verifies the definition directly:

    A history is serializable iff there is a total order of the committed
    transactions, consistent with real-time precedence, such that executing
    them serially in that order reproduces every recorded read result, starting
    from the empty initial state.

It enumerates permutations (small histories only), replays each transaction's
operations in the order the transaction recorded them (so read-your-own-writes
is reproduced), and compares the simulated reads with the recorded ones.  It
never calls the product's conflict detection.
"""

import itertools
import json
import os
import sqlite3

UNKNOWN = object()
TRUE = ("true",)
FALSE = ("false",)


def _atom(col, cmp_, value=None):
    return ("atom", col, cmp_, value)


def build(desc):
    if desc is None:
        return TRUE
    op = desc["op"]
    if op == "true":
        return TRUE
    if op == "false":
        return FALSE
    if op == "atom":
        return _atom(desc["column"], desc["cmp"], desc.get("value"))
    if op == "and":
        return ("and", tuple(build(p) for p in desc["parts"]))
    if op == "or":
        return ("or", tuple(build(p) for p in desc["parts"]))
    if op == "not":
        return ("not", build(desc["part"]))
    raise ValueError("bad predicate %r" % (desc,))


def _cmp3(value, op, operand):
    if value is None:
        return UNKNOWN
    if op == "eq":
        return value == operand
    if op == "lt":
        return value < operand
    if op == "le":
        return value <= operand
    raise ValueError("bad comparison %r" % (op,))


def eval3(pred, row):
    if pred is TRUE:
        return True
    if pred is FALSE:
        return False
    kind = pred[0]
    if kind == "atom":
        _, col, cmp_, value = pred
        if cmp_ == "is_null":
            return row.get(col) is None
        return _cmp3(row.get(col), cmp_, value)
    if kind == "and":
        values = [eval3(p, row) for p in pred[1]]
        if any(v is False for v in values):
            return False
        if any(v is UNKNOWN for v in values):
            return UNKNOWN
        return True
    if kind == "or":
        values = [eval3(p, row) for p in pred[1]]
        if any(v is True for v in values):
            return True
        if any(v is UNKNOWN for v in values):
            return UNKNOWN
        return False
    if kind == "not":
        inner = eval3(pred[1], row)
        return UNKNOWN if inner is UNKNOWN else (not inner)
    raise ValueError("bad predicate")


def matches(pred, rid, values):
    if values is None:
        return False
    row = dict(values)
    row["row_id"] = rid
    return eval3(pred, row) is True


def _connect_ro(path):
    uri = "file:%s?mode=ro" % path.replace("?", "%3f").replace("#", "%23")
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def load_history(db_path):
    """Reconstruct the committed history from durable evidence only."""
    conn = _connect_ro(os.path.abspath(db_path))
    try:
        commit_order = [r[0] for r in conn.execute(
            "SELECT user_txid FROM decisions WHERE decision='committed' "
            "ORDER BY commit_version")]
        ops = {}
        for r in conn.execute("SELECT user_txid, ord, kind, table_name, detail_json "
                              "FROM tx_ops ORDER BY user_txid, ord"):
            detail = json.loads(r["detail_json"])
            if r["kind"] == "read":
                ops.setdefault(r["user_txid"], []).append({
                    "kind": "read", "table": r["table_name"],
                    "predicate": build(json.loads(detail["predicate"])),
                    "columns": detail["columns"],
                    "rows": detail["returned"],
                })
            else:
                ops.setdefault(r["user_txid"], []).append({
                    "kind": "write", "table": r["table_name"],
                    "row_id": detail["row_id"], "op": detail["op"],
                    "old": detail["old_values"], "new": detail["new_values"],
                    "prior": detail.get("prior_values"),
                })
        tables = {}
        for r in conn.execute("SELECT table_name, column_name FROM table_columns "
                              "ORDER BY table_name, position"):
            tables.setdefault(r["table_name"], []).append(r["column_name"])
        points = {}
        times = {}
        for r in conn.execute(
                "SELECT t.user_txid, t.base_version, t.commit_version, t.start_wall, "
                "d.decided_wall FROM transactions t JOIN decisions d "
                "ON d.user_txid=t.user_txid WHERE d.decision='committed'"):
            has_writes = conn.execute(
                "SELECT 1 FROM committed_writes WHERE user_txid=? LIMIT 1",
                (r["user_txid"],)).fetchone() is not None
            points[r["user_txid"]] = ((r["commit_version"], 0) if has_writes
                                      else (r["base_version"], 1))
            times[r["user_txid"]] = (r["start_wall"], r["decided_wall"])
    finally:
        conn.close()

    real_time = []
    for a in times:
        for b in times:
            if a != b and times[a][1] < times[b][0]:
                real_time.append((a, b))

    return {
        "initial": {t: {} for t in tables},
        "transactions": commit_order,
        "ops": ops,
        "tables": tables,
        "serial_points": points,
        "real_time": real_time,
    }


def _apply_write(state, op):
    table = state.setdefault(op["table"], {})
    if op["op"] == "delete":
        table.pop(op["row_id"], None)
    else:
        table[op["row_id"]] = op["new"]


def _simulate_read(state, op):
    rows = []
    for rid in sorted(state.get(op["table"], {})):
        values = state[op["table"]][rid]
        if matches(op["predicate"], rid, values):
            projected = {"row_id": rid}
            for name in op["columns"]:
                if name != "row_id":
                    projected[name] = values[name]
            rows.append(projected)
    return rows


def _strip(rows):
    return [{k: v for k, v in row.items() if k != "version"} for row in rows]


def serial_valid(history, order):
    state = {t: dict(rows) for t, rows in history["initial"].items()}
    for txid in order:
        for op in history["ops"].get(txid, []):
            if op["kind"] == "write":
                current = state.get(op["table"], {}).get(op["row_id"])
                expected = op["prior"] if op["prior"] is not None else op["old"]
                if op["op"] == "insert":
                    if current is not None and expected is not None \
                            and current != expected:
                        return False, {"transaction": txid,
                                       "reason": "insert not applicable",
                                       "row_id": op["row_id"]}
                elif expected is not None and current != expected:
                    return False, {"transaction": txid, "reason": "stale write base",
                                   "row_id": op["row_id"], "expected": expected,
                                   "actual": current}
                _apply_write(state, op)
            else:
                observed = _simulate_read(state, op)
                recorded = _strip(op["rows"])
                if observed != recorded:
                    return False, {"transaction": txid, "table": op["table"],
                                   "predicate": str(op["predicate"]),
                                   "recorded": recorded, "replayed": observed}
    return True, None


def respects_real_time(history, order):
    position = {tx: i for i, tx in enumerate(order)}
    return all(position[a] < position[b] for a, b in history.get("real_time", []))


def claimed_serial_order(history):
    """The engine's claimed serialization points, read from durable evidence."""
    points = history["serial_points"]
    return tuple(sorted(history["transactions"], key=lambda t: (points[t], t)))


def enumerate_serial_orders(history):
    """Return (valid_orders, first_counterexample).  Small histories only."""
    txs = history["transactions"]
    valid = []
    first_bad = None
    for order in itertools.permutations(txs):
        if not respects_real_time(history, order):
            continue
        ok, detail = serial_valid(history, order)
        if ok:
            valid.append(order)
        elif first_bad is None:
            first_bad = (order, detail)
    return valid, first_bad
