"""Worker processes for the real multi-process drills.

Each worker is a standalone process.  The parent drives ordering with a
barrier/pipe protocol so the observed interleavings are genuinely concurrent
and reproducible.  Workers only talk to the database through ``mrtx``.

Protocol: a worker opens the inherited command pipe, reads one JSON command per
line, performs it, and writes one JSON result per line to the inherited result
pipe.  ``begin`` returns the transaction handle so the parent can keep the
transaction alive across commands in that process.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mrtx import Client, ConflictError, ManagedTxError  # noqa: E402
from mrtx.engine import TransactionHandle  # noqa: E402


def _pred(desc):
    """Rebuild a predicate from a small JSON description."""
    from mrtx import eq, lt, le, is_null, and_, or_, not_

    if desc is None:
        return None
    if isinstance(desc, str) and desc in ("true", "false"):
        return {"true": True, "false": False}[desc]
    op = desc["op"]
    if op == "eq":
        return eq(desc["col"], desc["value"])
    if op == "lt":
        return lt(desc["col"], desc["value"])
    if op == "le":
        return le(desc["col"], desc["value"])
    if op == "is_null":
        return is_null(desc["col"])
    if op == "and":
        return and_(*[_pred(p) for p in desc["parts"]])
    if op == "or":
        return or_(*[_pred(p) for p in desc["parts"]])
    if op == "not":
        return not_(_pred(desc["part"]))
    if op == "none":
        return None
    raise ValueError("bad predicate description %r" % (desc,))


def handle_command(client, handles, cmd):
    name = cmd["cmd"]
    if name == "begin":
        tx = client.begin(txid=cmd.get("txid"))
        handles[tx.txid] = tx
        return {"ok": True, "txid": tx.txid, "handle": tx.handle.as_dict()}
    if name == "read":
        tx = handles[cmd["txid"]]
        rows = tx.read(cmd["table"], columns=cmd.get("columns"),
                       where=_pred(cmd.get("where")))
        return {"ok": True, "rows": rows}
    if name == "insert":
        tx = handles[cmd["txid"]]
        return {"ok": True,
                "staged": tx.stage_insert(cmd["table"], cmd["row_id"], cmd["values"])}
    if name == "update":
        tx = handles[cmd["txid"]]
        return {"ok": True,
                "staged": tx.stage_update(cmd["table"], cmd["row_id"], cmd["values"],
                                          expected_version=cmd.get("expected_version"))}
    if name == "delete":
        tx = handles[cmd["txid"]]
        return {"ok": True,
                "staged": tx.stage_delete(cmd["table"], cmd["row_id"],
                                          expected_version=cmd.get("expected_version"))}
    if name == "commit":
        tx = handles[cmd["txid"]]
        return {"ok": True, "result": tx.commit()}
    if name == "commit_crash":
        # Simulate the committer dying after the decision is durable but
        # before any response reaches the caller.
        tx = handles[cmd["txid"]]
        tx.commit()
        os._exit(17)
    if name == "abort":
        tx = handles[cmd["txid"]]
        return {"ok": True, "result": tx.abort()}
    if name == "status":
        return {"ok": True, "status": client.transaction_status(cmd["txid"])}
    if name == "rows":
        return {"ok": True, "rows": client.rows(cmd["table"])}
    if name == "exit":
        return {"ok": True, "bye": True}
    raise ValueError("unknown command %r" % (name,))


def main():
    db_path = sys.argv[1]
    cmd_fd = int(sys.argv[2])
    res_fd = int(sys.argv[3])
    client = Client(db_path)
    handles = {}
    cmd_file = os.fdopen(cmd_fd, "r")
    res_file = os.fdopen(res_fd, "w")
    for line in cmd_file:
        line = line.strip()
        if not line:
            continue
        cmd = json.loads(line)
        try:
            result = handle_command(client, handles, cmd)
        except ConflictError as exc:
            result = {"ok": False, "error": "conflict", "code": exc.code,
                      "message": str(exc), "evidence": exc.evidence}
        except ManagedTxError as exc:
            result = {"ok": False, "error": type(exc).__name__, "message": str(exc)}
        except Exception as exc:  # noqa: BLE001 - report to parent
            result = {"ok": False, "error": "unexpected", "message": repr(exc)}
        res_file.write(json.dumps(result) + "\n")
        res_file.flush()
        if result.get("bye"):
            break


if __name__ == "__main__":
    main()
