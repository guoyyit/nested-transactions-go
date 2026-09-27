package tx

import "time"

// Default limits.
const (
	DefaultMaxDepth         = 8
	DefaultMaxSavepoints    = 64
	DefaultMaxRetries       = 8
	DefaultRetryBaseBackoff = time.Millisecond
)

// Options configures a Manager. Zero values are replaced by defaults.
type Options struct {
	// MaxDepth bounds transaction nesting (root = depth 1).
	MaxDepth int
	// MaxSavepoints bounds live savepoints per transaction.
	MaxSavepoints int
	// MaxRetries bounds automatic retries in Manager.Run on ErrConflict.
	MaxRetries int
	// RetryBaseBackoff is the base delay between Run retries.
	RetryBaseBackoff time.Duration
}

func (o Options) withDefaults() Options {
	if o.MaxDepth <= 0 {
		o.MaxDepth = DefaultMaxDepth
	}
	if o.MaxSavepoints <= 0 {
		o.MaxSavepoints = DefaultMaxSavepoints
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = DefaultMaxRetries
	}
	if o.RetryBaseBackoff <= 0 {
		o.RetryBaseBackoff = DefaultRetryBaseBackoff
	}
	return o
}
