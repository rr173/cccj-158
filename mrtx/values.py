"""Typed rows and three-valued predicates.

Column domains are int64, text, bool and null.  A declared non-null domain
still permits SQL NULL as a *value* (typed ``null``); a ``null``-declared
column only permits NULL.  ``bool`` and ``int`` are disjoint even though
Python's ``bool`` subclasses ``int``; every check tests ``bool`` first.  Text
ordering is Python's ``str`` order, i.e. Unicode code points, never locale.

Predicates use SQL three-valued logic.  ``read`` returns only rows for which
the predicate evaluates to TRUE; FALSE and UNKNOWN are both excluded.
"""

from .errors import SchemaError, PredicateError

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

TYPE_INT = "int64"
TYPE_TEXT = "text"
TYPE_BOOL = "bool"
TYPE_NULL = "null"
COLUMN_TYPES = frozenset({TYPE_INT, TYPE_TEXT, TYPE_BOOL, TYPE_NULL})

UNKNOWN = object()


def is_int64(value):
    return isinstance(value, int) and not isinstance(value, bool) and INT64_MIN <= value <= INT64_MAX


def is_text(value):
    return isinstance(value, str)


def is_bool(value):
    return isinstance(value, bool)


def check_value(value, coltype, context="value"):
    """Validate a value against its declared domain.

    NULL is accepted for every domain (it is the typed ``null`` value).  A
    ``null``-declared column accepts only NULL.
    """
    if value is None:
        return
    if coltype == TYPE_NULL:
        raise SchemaError("%s: column is declared null-only, got %r" % (context, value))
    if coltype == TYPE_INT:
        if not is_int64(value):
            raise SchemaError("%s: %r is not an int64" % (context, value))
    elif coltype == TYPE_TEXT:
        if not is_text(value):
            raise SchemaError("%s: %r is not text" % (context, value))
    elif coltype == TYPE_BOOL:
        if not is_bool(value):
            raise SchemaError("%s: %r is not a bool (bool and int are distinct)" % (context, value))
    else:
        raise SchemaError("%s: unknown column type %r" % (context, coltype))


# --- typed predicate AST ----------------------------------------------------


class Atom:
    __slots__ = ("column", "op", "value")

    def __init__(self, column, op, value=None):
        self.column = column
        self.op = op
        self.value = value

    def __repr__(self):
        if self.op == "is_null":
            return "Atom(%r, is_null)" % (self.column,)
        return "Atom(%r, %r, %r)" % (self.column, self.op, self.value)

    def __eq__(self, other):
        return (isinstance(other, Atom) and self.column == other.column
                and self.op == other.op and type(self.value) is type(other.value)
                and self.value == other.value)

    def __hash__(self):
        return hash((self.column, self.op, repr(self.value)))


class And:
    __slots__ = ("parts",)

    def __init__(self, parts):
        self.parts = tuple(parts)

    def __repr__(self):
        return "And(%r)" % (self.parts,)

    def __eq__(self, other):
        return isinstance(other, And) and self.parts == other.parts

    def __hash__(self):
        return hash(("and", self.parts))


class Or:
    __slots__ = ("parts",)

    def __init__(self, parts):
        self.parts = tuple(parts)

    def __repr__(self):
        return "Or(%r)" % (self.parts,)

    def __eq__(self, other):
        return isinstance(other, Or) and self.parts == other.parts

    def __hash__(self):
        return hash(("or", self.parts))


class Not:
    __slots__ = ("part",)

    def __init__(self, part):
        self.part = part

    def __repr__(self):
        return "Not(%r)" % (self.part,)

    def __eq__(self, other):
        return isinstance(other, Not) and self.part == other.part

    def __hash__(self):
        return hash(("not", self.part))


class _Literal:
    __slots__ = ()
    label = "literal"

    def __repr__(self):
        return self.label.upper()

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __hash__(self):
        return hash(self.label)


class LiteralTrue(_Literal):
    __slots__ = ()
    label = "true"


class LiteralFalse(_Literal):
    __slots__ = ()
    label = "false"


TRUE = LiteralTrue()
FALSE = LiteralFalse()

OPS = frozenset({"eq", "lt", "le", "is_null"})


def compile_predicate(pred, columns, allow_row_id=True):
    """Validate a public predicate against a table's {column: type} map."""
    if pred is None:
        return TRUE
    if isinstance(pred, (LiteralTrue, LiteralFalse)):
        return pred
    if isinstance(pred, Atom):
        if pred.column == "row_id":
            if not allow_row_id:
                raise PredicateError("row_id is not filterable here")
            if pred.op not in ("eq", "lt", "le", "is_null"):
                raise PredicateError("unknown predicate operator %r" % (pred.op,))
            if pred.op == "is_null":
                if pred.value is not None:
                    raise PredicateError("is_null takes no operand")
                return pred
            if not isinstance(pred.value, str):
                raise PredicateError("row_id comparisons must use text")
            return pred
        if pred.column not in columns:
            raise PredicateError("unknown column %r in predicate" % (pred.column,))
        if pred.op not in OPS:
            raise PredicateError("unknown predicate operator %r" % (pred.op,))
        coltype = columns[pred.column]
        if pred.op == "is_null":
            if pred.value is not None:
                raise PredicateError("is_null takes no operand")
            return pred
        if pred.value is None:
            raise PredicateError(
                "comparison operand for %r must not be null; use is_null" % (pred.column,))
        if coltype == TYPE_NULL:
            raise PredicateError("column %r is null-only; use is_null" % (pred.column,))
        check_value(pred.value, coltype, "predicate on %r" % (pred.column,))
        return pred
    if isinstance(pred, And):
        if not pred.parts:
            raise PredicateError("And requires at least one part (use TRUE)")
        return And([compile_predicate(p, columns, allow_row_id) for p in pred.parts])
    if isinstance(pred, Or):
        if not pred.parts:
            raise PredicateError("Or requires at least one part (use FALSE)")
        return Or([compile_predicate(p, columns, allow_row_id) for p in pred.parts])
    if isinstance(pred, Not):
        return Not(compile_predicate(pred.part, columns, allow_row_id))
    raise PredicateError(
        "predicate must be built from Atom/And/Or/Not/TRUE/FALSE, got %r"
        % (type(pred).__name__,))


# --- evaluation -------------------------------------------------------------


def _compare(value, op, operand):
    if value is None:
        return UNKNOWN
    if op == "eq":
        return value == operand
    if op == "lt":
        return value < operand
    if op == "le":
        return value <= operand
    raise PredicateError("unknown comparison %r" % (op,))


def _and(values):
    if any(v is False for v in values):
        return False
    if any(v is UNKNOWN for v in values):
        return UNKNOWN
    return True


def _or(values):
    if any(v is True for v in values):
        return True
    if any(v is UNKNOWN for v in values):
        return UNKNOWN
    return False


def evaluate(pred, row):
    """Evaluate a compiled predicate over a complete row mapping (3VL)."""
    if isinstance(pred, LiteralTrue):
        return True
    if isinstance(pred, LiteralFalse):
        return False
    if isinstance(pred, Atom):
        value = row.get(pred.column)
        if pred.op == "is_null":
            return value is None
        return _compare(value, pred.op, pred.value)
    if isinstance(pred, And):
        return _and([evaluate(p, row) for p in pred.parts])
    if isinstance(pred, Or):
        return _or([evaluate(p, row) for p in pred.parts])
    if isinstance(pred, Not):
        inner = evaluate(pred.part, row)
        if inner is UNKNOWN:
            return UNKNOWN
        return not inner
    raise PredicateError("uncompilable predicate %r" % (pred,))


def returns_true(pred, row):
    return evaluate(pred, row) is True


def columns_used(pred):
    if isinstance(pred, Atom):
        return {pred.column}
    if isinstance(pred, (And, Or)):
        out = set()
        for p in pred.parts:
            out |= columns_used(p)
        return out
    if isinstance(pred, Not):
        return columns_used(pred.part)
    return set()


# --- predicate builders -----------------------------------------------------


def eq(column, value):
    return Atom(column, "eq", value)


def lt(column, value):
    return Atom(column, "lt", value)


def le(column, value):
    return Atom(column, "le", value)


def is_null(column):
    return Atom(column, "is_null")


def and_(*parts):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = tuple(parts[0])
    return And(parts)


def or_(*parts):
    if len(parts) == 1 and isinstance(parts[0], (list, tuple)):
        parts = tuple(parts[0])
    return Or(parts)


def not_(part):
    return Not(part)
