package storage

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"time"
)

type Retryer struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64

	Metrics Metrics
	Logger  *slog.Logger
}

func NewRetryer(
	maxAttempts int,
	initialBackoff, maxBackoff time.Duration,
	multiplier float64,
	metrics Metrics,
	logger *slog.Logger,
) *Retryer {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if initialBackoff <= 0 {
		initialBackoff = 100 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}
	if multiplier <= 1 {
		multiplier = 2
	}
	if metrics == nil {
		metrics = NewNoopMetrics()
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Retryer{
		MaxAttempts:    maxAttempts,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
		Multiplier:     multiplier,
		Metrics:        metrics,
		Logger:         logger,
	}
}

// Do executes fn with bounded retry behavior.
// It only retries errors classified as retryable.
func (r *Retryer) Do(ctx context.Context, op string, fn func(ctx context.Context) error) error {
	var attempt int

	for {
		attempt++

		err := fn(ctx)
		if err == nil {
			return nil
		}

		if !IsRetryable(err) {
			return err
		}

		if attempt >= r.MaxAttempts {
			return err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return FromContextError(ctxErr, op, "", "")
		}

		delay := r.backoff(attempt)
		r.Metrics.RetryAttempt(op)
		r.Logger.Warn("retrying storage operation",
			slog.String("op", op),
			slog.Int("attempt", attempt),
			slog.Duration("delay", delay),
		)

		select {
		case <-ctx.Done():
			return FromContextError(ctx.Err(), op, "", "")
		case <-time.After(delay):
		}
	}
}

func (r *Retryer) backoff(attempt int) time.Duration {
	base := float64(r.InitialBackoff) * math.Pow(r.Multiplier, float64(attempt-1))
	if base > float64(r.MaxBackoff) {
		base = float64(r.MaxBackoff)
	}

	// Full jitter avoids synchronized retry storms.
	jittered := rand.Float64() * base
	if jittered <= 0 {
		jittered = float64(time.Millisecond)
	}

	return time.Duration(jittered)
}