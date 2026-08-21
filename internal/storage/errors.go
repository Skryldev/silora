package storage

import (
	"context"
	"errors"
)

// Kind classifies storage errors into stable domain categories.
type Kind int

const (
	KindUnknown Kind = iota
	KindNotFound
	KindAlreadyExists
	KindInvalidArgument
	KindUnauthorized
	KindForbidden
	KindTimeout
	KindCanceled
	KindUnavailable
	KindThrottled
	KindServerFailure
	KindConflict
	KindInternal
	KindClosed
	KindTooLarge
)

func (k Kind) Error() string {
	switch k {
	case KindNotFound:
		return "not found"
	case KindAlreadyExists:
		return "already exists"
	case KindInvalidArgument:
		return "invalid argument"
	case KindUnauthorized:
		return "unauthorized"
	case KindForbidden:
		return "forbidden"
	case KindTimeout:
		return "timeout"
	case KindCanceled:
		return "canceled"
	case KindUnavailable:
		return "storage unavailable"
	case KindThrottled:
		return "throttled"
	case KindServerFailure:
		return "server failure"
	case KindConflict:
		return "conflict"
	case KindInternal:
		return "internal storage error"
	case KindClosed:
		return "storage closed"
	case KindTooLarge:
		return "object too large"
	default:
		return "unknown storage error"
	}
}

var (
	ErrNotFound          = KindNotFound
	ErrAlreadyExists     = KindAlreadyExists
	ErrInvalidArgument   = KindInvalidArgument
	ErrUnauthorized      = KindUnauthorized
	ErrForbidden         = KindForbidden
	ErrTimeout           = KindTimeout
	ErrCanceled          = KindCanceled
	ErrStorageUnavailable = KindUnavailable
	ErrThrottled         = KindThrottled
	ErrServerFailure     = KindServerFailure
	ErrConflict          = KindConflict
	ErrInternal          = KindInternal
	ErrClosed            = KindClosed
	ErrTooLarge          = KindTooLarge
)

// Error is the domain error returned by all storage operations.
type Error struct {
	Kind   Kind
	Op     string
	Bucket string
	Key    string
	Err    error
}

func NewError(kind Kind, op, bucket, key string, err error) *Error {
	return &Error{
		Kind:   kind,
		Op:     op,
		Bucket: bucket,
		Key:    key,
		Err:    err,
	}
}

func (e *Error) Error() string {
	msg := "storage: " + e.Kind.Error()
	if e.Op != "" {
		msg += ": op=" + e.Op
	}
	if e.Bucket != "" {
		msg += ": bucket=" + e.Bucket
	}
	if e.Key != "" {
		msg += ": key=" + e.Key
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) Is(target error) bool {
	if t, ok := target.(Kind); ok {
		return e.Kind == t
	}

	var t *Error
	if errors.As(target, &t) {
		return t.Kind == e.Kind
	}

	return false
}

// IsRetryable reports whether an error is safe to retry.
//
// Context cancellation and caller deadline exhaustion are not retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var serr *Error
	if errors.As(err, &serr) {
		switch serr.Kind {
		case KindTimeout, KindUnavailable, KindThrottled, KindServerFailure:
			return true
		default:
			return false
		}
	}

	return false
}

func FromContextError(err error, op, bucket, key string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return NewError(KindCanceled, op, bucket, key, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return NewError(KindTimeout, op, bucket, key, err)
	}

	return NewError(KindUnknown, op, bucket, key, err)
}