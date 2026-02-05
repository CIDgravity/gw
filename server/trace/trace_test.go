package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGenerateTraceID(t *testing.T) {
	id1 := GenerateTraceID()
	id2 := GenerateTraceID()

	if id1 == "" {
		t.Error("GenerateTraceID returned empty string")
	}

	if id1 == id2 {
		t.Error("GenerateTraceID should return unique IDs")
	}

	// Should be 32 hex characters (16 bytes)
	if len(id1) != 32 {
		t.Errorf("Expected trace ID length 32, got %d", len(id1))
	}
}

func TestFromContext(t *testing.T) {
	ctx := context.Background()

	// Empty context should return empty string
	if id := FromContext(ctx); id != "" {
		t.Errorf("Expected empty string from empty context, got %q", id)
	}

	// Context with trace ID
	ctx = WithTraceID(ctx, "test-trace-id")
	if id := FromContext(ctx); id != "test-trace-id" {
		t.Errorf("Expected 'test-trace-id', got %q", id)
	}
}

func TestFromRequest(t *testing.T) {
	// Request with X-Trace-ID header
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TraceIDHeader, "header-trace-id")

	if id := FromRequest(req); id != "header-trace-id" {
		t.Errorf("Expected 'header-trace-id', got %q", id)
	}

	// Request with X-Request-ID header (fallback)
	req = httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(RequestIDHeader, "request-id")

	if id := FromRequest(req); id != "request-id" {
		t.Errorf("Expected 'request-id', got %q", id)
	}

	// Request without headers should generate new ID
	req = httptest.NewRequest("GET", "/test", nil)
	id := FromRequest(req)
	if id == "" {
		t.Error("Expected generated trace ID, got empty string")
	}
}

func TestMiddleware(t *testing.T) {
	var capturedTraceID string

	handler := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTraceID = FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	// Test with trace ID in request
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(TraceIDHeader, "middleware-trace-id")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if capturedTraceID != "middleware-trace-id" {
		t.Errorf("Expected 'middleware-trace-id' in context, got %q", capturedTraceID)
	}

	// Response should have trace ID header
	if respTraceID := w.Header().Get(TraceIDHeader); respTraceID != "middleware-trace-id" {
		t.Errorf("Expected trace ID in response header, got %q", respTraceID)
	}

	// Test without trace ID (should generate)
	req = httptest.NewRequest("GET", "/test", nil)
	w = httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if capturedTraceID == "" {
		t.Error("Expected generated trace ID in context")
	}

	if respTraceID := w.Header().Get(TraceIDHeader); respTraceID == "" {
		t.Error("Expected trace ID in response header")
	}
}

func TestLogFields(t *testing.T) {
	ctx := context.Background()

	// Empty context should return nil
	fields := LogFields(ctx)
	if fields != nil {
		t.Errorf("Expected nil from empty context, got %v", fields)
	}

	// Context with trace ID
	ctx = WithTraceID(ctx, "log-trace-id")
	fields = LogFields(ctx)

	if len(fields) != 2 {
		t.Fatalf("Expected 2 fields, got %d", len(fields))
	}

	if fields[0] != "trace_id" {
		t.Errorf("Expected first field to be 'trace_id', got %v", fields[0])
	}

	if fields[1] != "log-trace-id" {
		t.Errorf("Expected second field to be 'log-trace-id', got %v", fields[1])
	}
}

func TestWithLogFields(t *testing.T) {
	ctx := WithTraceID(context.Background(), "combined-trace-id")

	fields := WithLogFields(ctx, "key1", "value1", "key2", "value2")

	if len(fields) != 6 {
		t.Fatalf("Expected 6 fields, got %d", len(fields))
	}

	// First two should be trace_id
	if fields[0] != "trace_id" || fields[1] != "combined-trace-id" {
		t.Errorf("Expected trace_id first, got %v, %v", fields[0], fields[1])
	}

	// Rest should be the additional fields
	if fields[2] != "key1" || fields[4] != "key2" {
		t.Errorf("Expected additional fields preserved, got %v", fields)
	}
}

func TestInjectIntoRequest(t *testing.T) {
	ctx := WithTraceID(context.Background(), "inject-trace-id")

	req := httptest.NewRequest("GET", "/test", nil)
	InjectIntoRequest(ctx, req)

	if header := req.Header.Get(TraceIDHeader); header != "inject-trace-id" {
		t.Errorf("Expected trace ID in header, got %q", header)
	}

	// Empty context should not set header
	req = httptest.NewRequest("GET", "/test", nil)
	InjectIntoRequest(context.Background(), req)

	if header := req.Header.Get(TraceIDHeader); header != "" {
		t.Errorf("Expected empty header for empty context, got %q", header)
	}
}
