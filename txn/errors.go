package txn

import "errors"

var (
	// ErrConflict is returned when a transaction cannot be serialized
	// because a conflicting transaction committed first. Retrying is safe.
	ErrConflict = errors.New("txn: conflict with a concurrent transaction")
	// ErrTxnClosed is returned when using a transaction that already ended.
	ErrTxnClosed = errors.New("txn: transaction already closed")
	// ErrSavepointNotFound is returned when rolling back to or releasing a
	// savepoint that does not exist (or was invalidated by a rollback).
	ErrSavepointNotFound = errors.New("txn: savepoint not found")
	// ErrSavepointReleased is returned when using a savepoint that was
	// explicitly released.
	ErrSavepointReleased = errors.New("txn: savepoint already released")
	// ErrMaxDepth is returned when BeginChild would exceed the nesting limit.
	ErrMaxDepth = errors.New("txn: maximum nesting depth exceeded")
	// ErrTooManyRetries is returned by Run when conflicts keep recurring.
	ErrTooManyRetries = errors.New("txn: too many conflicts, giving up")
)
