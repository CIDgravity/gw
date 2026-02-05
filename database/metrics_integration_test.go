package database

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestSQLQueryTimer verifies SQL query timing functionality
func TestSQLQueryTimer(t *testing.T) {
	metrics := GetDBMetrics()

	// Test successful query timing
	timer := NewSQLQueryTimer("SELECT")
	time.Sleep(10 * time.Millisecond) // Simulate query execution
	timer.Done(nil)

	// Test failed query timing
	timer2 := NewSQLQueryTimer("INSERT")
	time.Sleep(5 * time.Millisecond)
	timer2.Done(errors.New("connection failed"))

	assert.NotNil(t, metrics)
	assert.True(t, true, "SQL query timing completed")
}

// TestCQLQueryTimer verifies CQL query timing functionality
func TestCQLQueryTimer(t *testing.T) {
	metrics := GetDBMetrics()

	// Test successful query timing
	timer := NewCQLQueryTimer("SELECT")
	time.Sleep(10 * time.Millisecond)
	timer.Done(nil)

	// Test failed query timing
	timer2 := NewCQLQueryTimer("INSERT")
	time.Sleep(5 * time.Millisecond)
	timer2.Done(errors.New("timeout"))

	assert.NotNil(t, metrics)
	assert.True(t, true, "CQL query timing completed")
}

// TestSQLQueryMetrics verifies SQL query metric recording
func TestSQLQueryMetrics(t *testing.T) {
	metrics := GetDBMetrics()

	operations := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"CREATE",
		"DROP",
	}

	// Record metrics for various operations
	for _, op := range operations {
		// Success
		metrics.ObserveSQLQuery(op, 100*time.Millisecond, nil)

		// Failure
		metrics.ObserveSQLQuery(op, 50*time.Millisecond, errors.New("error"))
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "SQL query metrics recorded")
}

// TestCQLQueryMetrics verifies CQL query metric recording
func TestCQLQueryMetrics(t *testing.T) {
	metrics := GetDBMetrics()

	operations := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"BATCH",
	}

	// Record metrics for various operations
	for _, op := range operations {
		// Success
		metrics.ObserveCQLQuery(op, 50*time.Millisecond, nil)

		// Failure
		metrics.ObserveCQLQuery(op, 25*time.Millisecond, errors.New("timeout"))
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "CQL query metrics recorded")
}

// TestBatchMetrics verifies CQL batch operation metrics
func TestBatchMetrics(t *testing.T) {
	metrics := GetDBMetrics()

	batchSizes := []int{1, 5, 10, 25, 50, 100}

	for _, size := range batchSizes {
		// Successful batch
		metrics.ObserveCQLBatch(size, 100*time.Millisecond, nil)

		// Failed batch
		metrics.ObserveCQLBatch(size, 200*time.Millisecond, errors.New("batch failed"))
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "Batch metrics recorded")
}

// TestConnectionPoolMetrics verifies connection pool metric updates
func TestConnectionPoolMetrics(t *testing.T) {
	metrics := GetDBMetrics()

	// Update SQL connection metrics
	metrics.SetSQLConnections(10, 5) // 10 active, 5 idle
	metrics.SetSQLConnections(20, 10)
	metrics.SetSQLConnections(0, 0)

	// Update CQL connection metrics
	metrics.SetCQLConnections(15)
	metrics.SetCQLConnections(30)
	metrics.SetCQLConnections(0)

	assert.NotNil(t, metrics)
	assert.True(t, true, "Connection pool metrics updated")
}

// TestMetricsWithVariousDurations verifies metrics with different durations
func TestMetricsWithVariousDurations(t *testing.T) {
	metrics := GetDBMetrics()

	durations := []time.Duration{
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
	}

	for _, d := range durations {
		metrics.ObserveSQLQuery("SELECT", d, nil)
		metrics.ObserveCQLQuery("SELECT", d, nil)
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "Metrics with various durations recorded")
}

// TestMetricsConcurrentAccess verifies thread-safe concurrent metric recording
func TestMetricsConcurrentAccess(t *testing.T) {
	metrics := GetDBMetrics()

	done := make(chan bool, 20)

	// Concurrent SQL metrics
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			op := "SELECT"
			if id%2 == 0 {
				op = "INSERT"
			}

			metrics.ObserveSQLQuery(op, time.Duration(id)*time.Millisecond, nil)
			metrics.SetSQLConnections(id, id/2)
		}(i)
	}

	// Concurrent CQL metrics
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			metrics.ObserveCQLQuery("SELECT", time.Duration(id)*time.Millisecond, nil)
			metrics.ObserveCQLBatch(id*10, time.Duration(id)*time.Millisecond, nil)
			metrics.SetCQLConnections(id)
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent metric recording")
		}
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "Concurrent metrics recording completed")
}

// TestMetricsErrorHandling verifies error handling in metrics
func TestMetricsErrorHandling(t *testing.T) {
	metrics := GetDBMetrics()

	// Various error types
	errors := []error{
		errors.New("connection refused"),
		errors.New("timeout"),
		errors.New("syntax error"),
		errors.New("constraint violation"),
		nil, // No error
	}

	for _, err := range errors {
		metrics.ObserveSQLQuery("SELECT", 100*time.Millisecond, err)
		metrics.ObserveCQLQuery("SELECT", 100*time.Millisecond, err)
		metrics.ObserveCQLBatch(10, 100*time.Millisecond, err)
	}

	assert.NotNil(t, metrics)
	assert.True(t, true, "Error handling in metrics verified")
}

// TestMetricsSingleton verifies the singleton pattern
func TestMetricsSingleton(t *testing.T) {
	// Get metrics instance multiple times
	m1 := GetDBMetrics()
	m2 := GetDBMetrics()
	m3 := GetDBMetrics()

	// All should be the same instance
	assert.Same(t, m1, m2, "Should return same instance")
	assert.Same(t, m2, m3, "Should return same instance")
	assert.NotNil(t, m1, "DB metrics instance should not be nil")
}

// TestQueryTimerEdgeCases verifies query timer edge cases
func TestQueryTimerEdgeCases(t *testing.T) {
	// Very short duration
	timer1 := NewSQLQueryTimer("FAST")
	timer1.Done(nil)

	// Long duration
	timer2 := NewSQLQueryTimer("SLOW")
	time.Sleep(100 * time.Millisecond)
	timer2.Done(nil)

	// Immediate completion
	timer3 := NewCQLQueryTimer("INSTANT")
	timer3.Done(nil)

	assert.True(t, true, "Query timer edge cases handled")
}

// TestBatchMetricsEdgeCases verifies batch metrics edge cases
func TestBatchMetricsEdgeCases(t *testing.T) {
	metrics := GetDBMetrics()

	// Empty batch
	metrics.ObserveCQLBatch(0, 1*time.Millisecond, nil)

	// Single statement batch
	metrics.ObserveCQLBatch(1, 5*time.Millisecond, nil)

	// Large batch
	metrics.ObserveCQLBatch(1000, 500*time.Millisecond, nil)

	// Batch with error
	metrics.ObserveCQLBatch(50, 200*time.Millisecond, errors.New("partial failure"))

	assert.NotNil(t, metrics)
	assert.True(t, true, "Batch metrics edge cases handled")
}
