// Package trace provides request tracing and correlation ID support.
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// ContextKey is the type for context keys used by this package.
type ContextKey string

const (
	// TraceIDKey is the context key for the trace ID.
	TraceIDKey ContextKey = "trace_id"

	// TraceIDHeader is the HTTP header name for trace IDs.
	TraceIDHeader = "X-Trace-ID"

	// RequestIDHeader is an alternative header name commonly used.
	RequestIDHeader = "X-Request-ID"
)

// GenerateTraceID generates a new random trace ID.
func GenerateTraceID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// Fallback to a simpler method if crypto/rand fails
		return "fallback-trace-id"
	}
	return hex.EncodeToString(b)
}

// FromContext extracts the trace ID from a context.
// Returns empty string if no trace ID is present.
func FromContext(ctx context.Context) string {
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
		return traceID
	}
	return ""
}

// WithTraceID returns a new context with the given trace ID.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// FromRequest extracts or generates a trace ID from an HTTP request.
// It checks the X-Trace-ID and X-Request-ID headers, or generates a new one.
func FromRequest(r *http.Request) string {
	// Check for existing trace ID in headers
	traceID := r.Header.Get(TraceIDHeader)
	if traceID != "" {
		return traceID
	}

	traceID = r.Header.Get(RequestIDHeader)
	if traceID != "" {
		return traceID
	}

	// Generate a new trace ID
	return GenerateTraceID()
}

// InjectIntoRequest adds the trace ID from context into an outgoing HTTP request.
func InjectIntoRequest(ctx context.Context, req *http.Request) {
	traceID := FromContext(ctx)
	if traceID != "" {
		req.Header.Set(TraceIDHeader, traceID)
	}
}

// Middleware returns an HTTP middleware that extracts or generates trace IDs
// and adds them to the request context.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := FromRequest(r)

		// Add trace ID to response header for debugging
		w.Header().Set(TraceIDHeader, traceID)

		// Add trace ID to context
		ctx := WithTraceID(r.Context(), traceID)

		// Continue with the request
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// LogFields returns the trace fields for use with structured logging.
// Example: log.Infow("message", trace.LogFields(ctx)...)
func LogFields(ctx context.Context) []interface{} {
	traceID := FromContext(ctx)
	if traceID == "" {
		return nil
	}
	return []interface{}{"trace_id", traceID}
}

// WithLogFields appends trace fields to existing log fields.
// Example: log.Infow("message", trace.WithLogFields(ctx, "key", "value")...)
func WithLogFields(ctx context.Context, fields ...interface{}) []interface{} {
	traceFields := LogFields(ctx)
	if len(traceFields) == 0 {
		return fields
	}
	return append(traceFields, fields...)
}
