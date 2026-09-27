package tx

import "errors"

var (
	// ErrConflict is returned at commit time when the transaction's read
	// set was invalidated by a concurrent commit (OCC validation failure).
	ErrConflict = errors.New("tx: write conflict, transaction aborted")
	// ErrTxClosed is returned when operating on a committed/rolled-back tx.
	ErrTxClosed = errors.New("tx: transaction is closed")
	// ErrManagerClosed is returned when the manager has been closed.
	ErrManagerClosed = errors.New("tx: manager is closed")
	// ErrMaxDepth is returned when nesting would exceed Options.MaxDepth.
	ErrMaxDepth = errors.New("tx: maximum nesting depth exceeded")
	// ErrSavepointExists is returned when redefining an existing savepoint.
	ErrSavepointExists = errors.New("tx: savepoint already exists")
	// ErrSavepointNotFound is returned for unknown/released/destroyed savepoints.
	ErrSavepointNotFound = errors.New("tx: savepoint not found or already released")
	// ErrTooManySavepoints is returned past Options.MaxSavepoints per tx.
	ErrTooManySavepoints = errors.New("tx: savepoint limit reached")
)
