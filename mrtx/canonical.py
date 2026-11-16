"""Canonical (stable, deterministic) encoding of predicates and values."""

import json

from .values import (And, Atom, FALSE, LiteralFalse, LiteralTrue, Not, Or,
                     TRUE, columns_used)


def predicate_to_json(pred):
    if isinstance(pred, Atom):
        return {"op": "atom", "column": pred.column, "cmp": pred.op, "value": pred.value}
    if isinstance(pred, And):
        return {"op": "and", "parts": [predicate_to_json(p) for p in pred.parts]}
    if isinstance(pred, Or):
        return {"op": "or", "parts": [predicate_to_json(p) for p in pred.parts]}
    if isinstance(pred, Not):
        return {"op": "not", "part": predicate_to_json(pred.part)}
    if isinstance(pred, LiteralTrue):
        return {"op": "true"}
    if isinstance(pred, LiteralFalse):
        return {"op": "false"}
    raise TypeError("cannot serialize predicate %r" % (pred,))


def predicate_from_json(obj):
    op = obj["op"]
    if op == "atom":
        return Atom(obj["column"], obj["cmp"], obj.get("value"))
    if op == "and":
        return And([predicate_from_json(p) for p in obj["parts"]])
    if op == "or":
        return Or([predicate_from_json(p) for p in obj["parts"]])
    if op == "not":
        return Not(predicate_from_json(obj["part"]))
    if op == "true":
        return TRUE
    if op == "false":
        return FALSE
    raise ValueError("bad predicate encoding %r" % (obj,))


def predicate_text(pred):
    return json.dumps(predicate_to_json(pred), sort_keys=True, separators=(",", ":"))


def describe(pred):
    """Human-readable predicate plus the exact columns it depends on."""
    return {"expression": _describe(pred), "columns": sorted(columns_used(pred))}


def _describe(pred):
    if isinstance(pred, Atom):
        if pred.op == "is_null":
            return "%s IS NULL" % pred.column
        return "%s %s %s" % (pred.column, {"eq": "=", "lt": "<", "le": "<="}[pred.op],
                             json.dumps(pred.value))
    if isinstance(pred, And):
        return "(" + " AND ".join(_describe(p) for p in pred.parts) + ")"
    if isinstance(pred, Or):
        return "(" + " OR ".join(_describe(p) for p in pred.parts) + ")"
    if isinstance(pred, Not):
        return "(NOT " + _describe(pred.part) + ")"
    if isinstance(pred, LiteralTrue):
        return "TRUE"
    return "FALSE"


def values_to_json(values):
    return json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def values_from_json(text):
    return json.loads(text)
