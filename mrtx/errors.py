"""Public exception types for the managed-repository transaction layer."""


class ManagedTxError(Exception):
    """Base class for all errors raised by mrtx."""


class SchemaError(ManagedTxError):
    """The caller referenced an unknown table/column/type or malformed predicate."""


class PredicateError(SchemaError):
    """A read predicate was not a well-formed three-valued expression."""


class TransactionError(ManagedTxError):
    """An operation was attempted in an invalid transaction state."""


class TransactionNotFound(TransactionError):
    """No such transaction id exists in this repository."""


class ConflictError(ManagedTxError):
    """Commit was refused because the transaction is not serializable.

    The exception carries structured, auditable evidence: the conflicting
    transaction, the kind of dependency, and the concrete row identity or
    predicate that witnesses it.  It is *not* a generic "stale version".
    """

    def __init__(self, message, code, evidence):
        super().__init__(message)
        self.code = code
        self.evidence = evidence

    def as_dict(self):
        return {"error": self.code, "message": str(self), "evidence": self.evidence}


class BusyError(ManagedTxError):
    """Internal SQLite lock contention that survived bounded retries.

    This is an infrastructure condition, never a transaction conflict.  It is
    surfaced as an internal error; the commit decision remains untouched.
    """


class InternalInvariantError(ManagedTxError):
    """A consistency invariant of the store was violated."""
