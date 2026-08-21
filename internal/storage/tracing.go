package storage

import "context"

// Tracer is an integration point for OpenTelemetry or other tracing systems.
//
// Phase 1 intentionally does not hard-depend on OpenTelemetry.
type Tracer interface {
	Start(ctx context.Context, operation string, attrs map[string]any) (context.Context, func(error))
}

type NoopTracer struct{}

func NewNoopTracer() *NoopTracer { return &NoopTracer{} }

func (NoopTracer) Start(ctx context.Context, _ string, _ map[string]any) (context.Context, func(error)) {
	return ctx, func(error) {}
}