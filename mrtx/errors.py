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


class SubscriptionError(ManagedTxError):
    """A subscription operation is invalid for its durable state.

    Raised for a closed subscription, a stale generation, a wrong token, an
    acknowledgement that does not match the single pending batch, or an
    attempt to advance state with a credential that belongs elsewhere.
    """


class SubscriptionNotFound(SubscriptionError):
    """No subscription with that name/id exists in this source repository."""


class ReplicaError(ManagedTxError):
    """A replica operation violated the replica's local protocol."""


class ReplicaNotReady(ReplicaError):
    """Business reads were attempted before the full delivery completed."""


class ReplicaGenerationError(ReplicaError):
    """The replica holds another subscription generation.

    Reinitialisation is explicit: the caller must decide to discard the old
    full load instead of mixing it with a new generation's full load.
    """


class SubscriptionClosed(SubscriptionError):
    """The subscription was closed; it cannot deliver more batches."""


class StaleSubscriptionGeneration(SubscriptionError):
    """A batch/acknowledgement belongs to a superseded subscription generation."""


class SchemaConflictError(ManagedTxError):
    """Commit refused because a migration changed a table the transaction writes.

    A transaction's start view fixes not only the data snapshot but also the
    column definition of every table it may touch.  When an online
    :meth:`Client.add_column` migration lands between a transaction's begin and
    its commit, that transaction may still read using its original column
    definition.  If it tries to *write* an affected table, the commit is
    rejected with this explicit, evidence-carrying result and leaves no write
    behind.
    """

    def __init__(self, message, code, evidence):
        super().__init__(message)
        self.code = code
        self.evidence = evidence

    def as_dict(self):
        return {"error": self.code, "message": str(self), "evidence": self.evidence}


class ReferenceError(SchemaError):
    """A reference-constraint registration was malformed or unsupported.

    Raised for a duplicate constraint name, an unknown table or column, a
    non-text referencing column, an unsupported delete strategy, or an attempt
    to register a rule that existing committed data already violates.  The
    registration transaction is rolled back, so no half definition remains.
    Commit-time violations of an already-registered rule are not raised here:
    they are :class:`ConflictError` rejections carrying reference evidence.
    """

