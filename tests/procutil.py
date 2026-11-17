"""Barrier-controlled worker processes for real multi-process drills."""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(HERE, "worker.py")


class Worker:
    """A real OS process running ``worker.py``; commands are JSON lines."""

    def __init__(self, db_path):
        cmd_r, cmd_w = os.pipe()
        res_r, res_w = os.pipe()
        try:
            self.proc = subprocess.Popen(
                [sys.executable, "-I", WORKER, db_path, str(cmd_r), str(res_w)],
                pass_fds=(cmd_r, res_w),
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, close_fds=True,
            )
        finally:
            os.close(cmd_r)
            os.close(res_w)
        self._cmd = os.fdopen(cmd_w, "w")
        self._res = os.fdopen(res_r, "r")
        self.pid = self.proc.pid
        self._txids = []
        self.handles = {}
        self._closed = False
        self._closing = False

    def send(self, cmd):
        self._cmd.write(json.dumps(cmd) + "\n")
        self._cmd.flush()
        line = self._res.readline()
        if not line:
            err = self.proc.stderr.read().decode("utf-8", "replace")
            self._closing = True
            raise RuntimeError("worker %d died: %s" % (self.pid, err))
        return json.loads(line)

    # convenience wrappers ---------------------------------------------------

    def begin(self):
        out = self.send({"cmd": "begin"})
        assert out["ok"], out
        self._txids.append(out["txid"])
        self.handles[out["txid"]] = out["handle"]
        return out["txid"]

    def read(self, txid, table, columns=None, where=None):
        return self.send({"cmd": "read", "txid": txid, "table": table,
                          "columns": columns, "where": where})

    def insert(self, txid, table, row_id, values):
        return self.send({"cmd": "insert", "txid": txid, "table": table,
                          "row_id": row_id, "values": values})

    def update(self, txid, table, row_id, values, expected_version=None):
        return self.send({"cmd": "update", "txid": txid, "table": table,
                          "row_id": row_id, "values": values,
                          "expected_version": expected_version})

    def delete(self, txid, table, row_id, expected_version=None):
        return self.send({"cmd": "delete", "txid": txid, "table": table,
                          "row_id": row_id, "expected_version": expected_version})

    def commit(self, txid):
        return self.send({"cmd": "commit", "txid": txid})

    def abort(self, txid):
        return self.send({"cmd": "abort", "txid": txid})

    def status(self, txid):
        return self.send({"cmd": "status", "txid": txid})

    def rows(self, table):
        return self.send({"cmd": "rows", "table": table})["rows"]

    def create_subscription(self, name, tables):
        out = self.send({"cmd": "create_subscription", "name": name,
                         "tables": tables})
        assert out["ok"], out
        return out

    def add_column(self, table, column, ctype, default):
        return self.send({"cmd": "add_column", "table": table,
                          "column": column, "type": ctype, "default": default})

    def register_reference(self, name, parent_table, child_table, child_column,
                           on_delete):
        return self.send({"cmd": "register_reference", "constraint": name,
                          "parent_table": parent_table, "child_table": child_table,
                          "child_column": child_column, "on_delete": on_delete})

    def list_references(self):
        return self.send({"cmd": "list_references"})["result"]

    def get_reference(self, name):
        return self.send({"cmd": "get_reference", "constraint": name})["result"]

    def schema_history(self, table=None):
        return self.send({"cmd": "schema_history", "table": table})["result"]

    def pull(self, handle, max_rows=100):
        return self.send({"cmd": "pull", "handle": handle,
                          "max_rows": max_rows})

    def pull_ack(self, handle, max_rows=100):
        return self.send({"cmd": "pull_ack", "handle": handle,
                          "max_rows": max_rows})

    def subscription_status(self, handle):
        return self.send({"cmd": "subscription_status", "handle": handle})["status"]

    def cleanup(self):
        return self.send({"cmd": "cleanup"})["report"]

    def gc(self):
        return self.send({"cmd": "gc"})["report"]

    def close(self):
        if self._closed:
            return
        self._closed = True
        if not self._closing:
            try:
                self.send({"cmd": "exit"})
            except Exception:
                pass
        for stream in (self._cmd, self._res, self.proc.stderr):
            try:
                stream.close()
            except Exception:
                pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def kill(self):
        """Simulate a crash: SIGKILL without cleanup."""
        self.proc.kill()
        self.proc.wait()
        self._closing = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
