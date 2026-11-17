"""Independent naive join-aggregate reference for materialized-summary tests.

This module deliberately imports nothing from the product's materializer and
never opens its auxiliary tables.  Given plain source-table row mappings (the
kind a test obtains through the public ``rows`` API), it computes the join and
the grouped aggregates in the most direct possible Python.  Randomized tests
compare its output, group tuple by group tuple, against the maintained
materialized table after every source commit.

Semantics mirrored exactly:

* exact non-null text equality on the join columns; nulls never match;
* no de-duplication -- every left/right row pair with the same key is a row;
* ``left`` keeps an unmatched left row with the right side entirely null;
* ``COUNT(*)`` counts join rows, ``COUNT(col)`` ignores nulls, ``SUM`` ignores
  nulls and is null when every value was null;
* group keys are typed tuples (bool vs int, null vs "" all distinct).
"""


def join_rows(left, right, left_key, right_key, join_kind):
    """Yield (left_values_or_None, right_values_or_None) join result rows."""
    right_by_key = {}
    for rid, vals in right.items():
        key = vals[right_key]
        if key is None:
            continue
        right_by_key.setdefault(key, []).append(vals)
    for _lid, lvals in left.items():
        key = lvals[left_key]
        if key is None:
            if join_kind == "left":
                yield lvals, None
            continue
        matches = right_by_key.get(key)
        if not matches:
            if join_kind == "left":
                yield lvals, None
            continue
        for rvals in matches:
            yield lvals, rvals


def _group_value(group_spec, lvals, rvals):
    out = []
    for spec in group_spec:
        vals = lvals if spec["side"] == "left" else rvals
        out.append(None if vals is None else vals.get(spec["column"]))
    return tuple(out)


def naive_summary(definition, left, right):
    """Return ``{group_tuple: {alias: aggregate_value}}`` from plain row maps."""
    groups = {}
    for lvals, rvals in join_rows(
            left, right, definition["left_join_column"],
            definition["right_join_column"], definition["join_kind"]):
        key = _group_value(definition["groups"], lvals, rvals)
        slot = groups.setdefault(key, {})
        for agg in definition["aggregates"]:
            alias = agg["alias"]
            if agg["fn"] == "count_star":
                slot[alias] = slot.get(alias, 0) + 1
                continue
            vals = lvals if agg["side"] == "left" else rvals
            value = None if vals is None else vals.get(agg["column"])
            if agg["fn"] == "count":
                slot[alias] = slot.get(alias, 0) + (1 if value is not None else 0)
            else:
                cur = slot.setdefault(alias, {"sum": None})
                if value is not None:
                    cur["sum"] = value if cur["sum"] is None else cur["sum"] + value
    # Finalize sum dicts into plain values.
    out = {}
    for key, slot in groups.items():
        finalized = {}
        for agg in definition["aggregates"]:
            value = slot.get(agg["alias"])
            if agg["fn"] == "sum":
                finalized[agg["alias"]] = None if value is None else value["sum"]
            else:
                finalized[agg["alias"]] = value if value is not None else 0
        out[key] = finalized
    return out


def definition_from_status(status):
    """The subset of a durable definition this reference understands."""
    return status["definition"]
