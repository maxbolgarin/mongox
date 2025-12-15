package mongox

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestAsyncError_Error(t *testing.T) {
	err := &AsyncError{
		Err:            errors.New("test error"),
		IsNotRetryable: true,
		Collection:     "users",
		Operation:      "insert_one",
		TaskName:       "insert_user",
		QueueKey:       "queue1",
		Timestamp:      time.Now(),
		RetryCount:     0,
	}

	expected := "async error in users.insert_one [insert_user]: test error"
	if got := err.Error(); got != expected {
		t.Errorf("AsyncError.Error() = %v, want %v", got, expected)
	}
}

func TestAsyncError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	err := &AsyncError{
		Err:            originalErr,
		IsNotRetryable: true,
		Collection:     "users",
		Operation:      "insert_one",
		TaskName:       "task1",
	}

	if unwrapped := err.Unwrap(); unwrapped != originalErr {
		t.Errorf("AsyncError.Unwrap() = %v, want %v", unwrapped, originalErr)
	}

	// Test errors.Is compatibility
	if !errors.Is(err, originalErr) {
		t.Error("errors.Is should return true for the original error")
	}
}

func TestAsyncErrorStats_Record(t *testing.T) {
	stats := newAsyncErrorStats()

	// Record a non-retryable error
	err1 := &AsyncError{
		Err:            errors.New("not found"),
		IsNotRetryable: true,
		Collection:     "users",
		Operation:      "find_one",
		TaskName:       "task1",
		QueueKey:       "queue1",
		Timestamp:      time.Now(),
	}
	stats.record(err1)

	// Record a retry-exhausted error (IsNotRetryable = false means retry exhausted)
	err2 := &AsyncError{
		Err:            errors.New("network error"),
		IsNotRetryable: false,
		Collection:     "orders",
		Operation:      "insert_one",
		TaskName:       "task2",
		QueueKey:       "queue2",
		Timestamp:      time.Now(),
		RetryCount:     10,
	}
	stats.record(err2)

	// Record another error for the same collection
	err3 := &AsyncError{
		Err:            errors.New("duplicate"),
		IsNotRetryable: true,
		Collection:     "users",
		Operation:      "insert_one",
		TaskName:       "task3",
		QueueKey:       "queue1",
		Timestamp:      time.Now(),
	}
	stats.record(err3)

	snapshot := stats.snapshot()

	if snapshot.TotalErrors != 3 {
		t.Errorf("TotalErrors = %d, want 3", snapshot.TotalErrors)
	}
	if snapshot.NonRetryableErrors != 2 {
		t.Errorf("NonRetryableErrors = %d, want 2", snapshot.NonRetryableErrors)
	}
	if snapshot.RetryExhaustedErrors != 1 {
		t.Errorf("RetryExhaustedErrors = %d, want 1", snapshot.RetryExhaustedErrors)
	}

	// Check collection stats
	usersStats, ok := snapshot.ByCollection["users"]
	if !ok {
		t.Fatal("users collection stats not found")
	}
	if usersStats.Total != 2 {
		t.Errorf("users.Total = %d, want 2", usersStats.Total)
	}
	if usersStats.NonRetryable != 2 {
		t.Errorf("users.NonRetryable = %d, want 2", usersStats.NonRetryable)
	}

	ordersStats, ok := snapshot.ByCollection["orders"]
	if !ok {
		t.Fatal("orders collection stats not found")
	}
	if ordersStats.Total != 1 {
		t.Errorf("orders.Total = %d, want 1", ordersStats.Total)
	}
	if ordersStats.RetryExhausted != 1 {
		t.Errorf("orders.RetryExhausted = %d, want 1", ordersStats.RetryExhausted)
	}

	// Check operation stats
	insertStats, ok := snapshot.ByOperation["insert_one"]
	if !ok {
		t.Fatal("insert_one operation stats not found")
	}
	if insertStats.Total != 2 {
		t.Errorf("insert_one.Total = %d, want 2", insertStats.Total)
	}

	findStats, ok := snapshot.ByOperation["find_one"]
	if !ok {
		t.Fatal("find_one operation stats not found")
	}
	if findStats.Total != 1 {
		t.Errorf("find_one.Total = %d, want 1", findStats.Total)
	}
}

func TestAsyncErrorStats_Snapshot_TopErrors(t *testing.T) {
	stats := newAsyncErrorStats()

	// Record multiple errors with the same message
	for i := 0; i < 15; i++ {
		stats.record(&AsyncError{
			Err:            errors.New("error A"),
			IsNotRetryable: true,
			Collection:     "coll",
			Operation:      "op",
		})
	}

	for i := 0; i < 10; i++ {
		stats.record(&AsyncError{
			Err:            errors.New("error B"),
			IsNotRetryable: true,
			Collection:     "coll",
			Operation:      "op",
		})
	}

	for i := 0; i < 5; i++ {
		stats.record(&AsyncError{
			Err:            errors.New("error C"),
			IsNotRetryable: true,
			Collection:     "coll",
			Operation:      "op",
		})
	}

	snapshot := stats.snapshot()

	if len(snapshot.TopErrors) != 3 {
		t.Errorf("TopErrors length = %d, want 3", len(snapshot.TopErrors))
	}

	// Verify top errors are sorted by count
	if snapshot.TopErrors[0].Error != "error A" || snapshot.TopErrors[0].Count != 15 {
		t.Errorf("TopErrors[0] = %v, want error A with count 15", snapshot.TopErrors[0])
	}
	if snapshot.TopErrors[1].Error != "error B" || snapshot.TopErrors[1].Count != 10 {
		t.Errorf("TopErrors[1] = %v, want error B with count 10", snapshot.TopErrors[1])
	}
	if snapshot.TopErrors[2].Error != "error C" || snapshot.TopErrors[2].Count != 5 {
		t.Errorf("TopErrors[2] = %v, want error C with count 5", snapshot.TopErrors[2])
	}
}

func TestAsyncErrorStats_Reset(t *testing.T) {
	stats := newAsyncErrorStats()

	// Record some errors
	for i := 0; i < 5; i++ {
		stats.record(&AsyncError{
			Err:            errors.New("test error"),
			IsNotRetryable: true,
			Collection:     "coll",
			Operation:      "op",
		})
	}

	snapshot := stats.snapshot()
	if snapshot.TotalErrors != 5 {
		t.Errorf("TotalErrors before reset = %d, want 5", snapshot.TotalErrors)
	}

	stats.reset()

	snapshot = stats.snapshot()
	if snapshot.TotalErrors != 0 {
		t.Errorf("TotalErrors after reset = %d, want 0", snapshot.TotalErrors)
	}
	if snapshot.NonRetryableErrors != 0 {
		t.Errorf("NonRetryableErrors after reset = %d, want 0", snapshot.NonRetryableErrors)
	}
	if snapshot.RetryExhaustedErrors != 0 {
		t.Errorf("RetryExhaustedErrors after reset = %d, want 0", snapshot.RetryExhaustedErrors)
	}
	if len(snapshot.ByCollection) != 0 {
		t.Errorf("ByCollection after reset = %d entries, want 0", len(snapshot.ByCollection))
	}
	if len(snapshot.ByOperation) != 0 {
		t.Errorf("ByOperation after reset = %d entries, want 0", len(snapshot.ByOperation))
	}
	if len(snapshot.TopErrors) != 0 {
		t.Errorf("TopErrors after reset = %d entries, want 0", len(snapshot.TopErrors))
	}
}

func TestAsyncErrorStats_Concurrent(t *testing.T) {
	stats := newAsyncErrorStats()
	var wg sync.WaitGroup
	numGoroutines := 100
	errorsPerGoroutine := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < errorsPerGoroutine; j++ {
				stats.record(&AsyncError{
					Err:            errors.New("concurrent error"),
					IsNotRetryable: true,
					Collection:     "coll",
					Operation:      "op",
				})
			}
		}(i)
	}

	wg.Wait()

	snapshot := stats.snapshot()
	expectedTotal := int64(numGoroutines * errorsPerGoroutine)
	if snapshot.TotalErrors != expectedTotal {
		t.Errorf("TotalErrors = %d, want %d", snapshot.TotalErrors, expectedTotal)
	}
}

func TestAsyncErrorStats_EmptySnapshot(t *testing.T) {
	stats := newAsyncErrorStats()
	snapshot := stats.snapshot()

	if snapshot.TotalErrors != 0 {
		t.Errorf("TotalErrors = %d, want 0", snapshot.TotalErrors)
	}
	if snapshot.NonRetryableErrors != 0 {
		t.Errorf("NonRetryableErrors = %d, want 0", snapshot.NonRetryableErrors)
	}
	if snapshot.RetryExhaustedErrors != 0 {
		t.Errorf("RetryExhaustedErrors = %d, want 0", snapshot.RetryExhaustedErrors)
	}
	if len(snapshot.ByCollection) != 0 {
		t.Errorf("ByCollection should be empty, got %d entries", len(snapshot.ByCollection))
	}
	if len(snapshot.ByOperation) != 0 {
		t.Errorf("ByOperation should be empty, got %d entries", len(snapshot.ByOperation))
	}
	if len(snapshot.TopErrors) != 0 {
		t.Errorf("TopErrors should be empty, got %d entries", len(snapshot.TopErrors))
	}
}

func TestNewAsyncErrorStats(t *testing.T) {
	stats := newAsyncErrorStats()

	if stats == nil {
		t.Fatal("newAsyncErrorStats returned nil")
	}
	if stats.byCollection == nil {
		t.Error("byCollection should not be nil")
	}
	if stats.byOperation == nil {
		t.Error("byOperation should not be nil")
	}
	if stats.byErrorType == nil {
		t.Error("byErrorType should not be nil")
	}
}

func TestAsyncErrorStats_TopErrorsLimit(t *testing.T) {
	stats := newAsyncErrorStats()

	// Record more than 10 different error types
	for i := 0; i < 15; i++ {
		for j := 0; j < (15 - i); j++ { // More occurrences for lower i
			stats.record(&AsyncError{
				Err:            errors.New("error " + string(rune('A'+i))),
				IsNotRetryable: true,
				Collection:     "coll",
				Operation:      "op",
			})
		}
	}

	snapshot := stats.snapshot()

	// Should only have top 10
	if len(snapshot.TopErrors) != 10 {
		t.Errorf("TopErrors length = %d, want 10", len(snapshot.TopErrors))
	}

	// First should be the most frequent
	if snapshot.TopErrors[0].Count < snapshot.TopErrors[9].Count {
		t.Error("TopErrors should be sorted by count descending")
	}
}

func TestAsyncError_Fields(t *testing.T) {
	timestamp := time.Now()
	err := &AsyncError{
		Err:            errors.New("test"),
		Collection:     "users",
		Operation:      "insert_one",
		TaskName:       "task1",
		QueueKey:       "queue1",
		Timestamp:      timestamp,
		RetryCount:     5,
		IsNotRetryable: true,
	}

	if err.Collection != "users" {
		t.Errorf("Collection = %s, want users", err.Collection)
	}
	if err.Operation != "insert_one" {
		t.Errorf("Operation = %s, want insert_one", err.Operation)
	}
	if err.TaskName != "task1" {
		t.Errorf("TaskName = %s, want task1", err.TaskName)
	}
	if err.QueueKey != "queue1" {
		t.Errorf("QueueKey = %s, want queue1", err.QueueKey)
	}
	if err.Timestamp != timestamp {
		t.Errorf("Timestamp = %v, want %v", err.Timestamp, timestamp)
	}
	if err.RetryCount != 5 {
		t.Errorf("RetryCount = %d, want 5", err.RetryCount)
	}
	if !err.IsNotRetryable {
		t.Error("IsNotRetryable should be true")
	}
}

func TestCollectionErrorStats_Fields(t *testing.T) {
	stats := CollectionErrorStats{
		Total:          10,
		NonRetryable:   7,
		RetryExhausted: 3,
	}

	if stats.Total != 10 {
		t.Errorf("Total = %d, want 10", stats.Total)
	}
	if stats.NonRetryable != 7 {
		t.Errorf("NonRetryable = %d, want 7", stats.NonRetryable)
	}
	if stats.RetryExhausted != 3 {
		t.Errorf("RetryExhausted = %d, want 3", stats.RetryExhausted)
	}
}

func TestOperationErrorStats_Fields(t *testing.T) {
	stats := OperationErrorStats{
		Total:          20,
		NonRetryable:   15,
		RetryExhausted: 5,
	}

	if stats.Total != 20 {
		t.Errorf("Total = %d, want 20", stats.Total)
	}
	if stats.NonRetryable != 15 {
		t.Errorf("NonRetryable = %d, want 15", stats.NonRetryable)
	}
	if stats.RetryExhausted != 5 {
		t.Errorf("RetryExhausted = %d, want 5", stats.RetryExhausted)
	}
}

func TestErrorCount_Fields(t *testing.T) {
	ec := ErrorCount{
		Error: "test error",
		Count: 42,
	}

	if ec.Error != "test error" {
		t.Errorf("Error = %s, want test error", ec.Error)
	}
	if ec.Count != 42 {
		t.Errorf("Count = %d, want 42", ec.Count)
	}
}

func TestAsyncErrorStatsSnapshot_Fields(t *testing.T) {
	snapshot := AsyncErrorStats{
		TotalErrors:          100,
		NonRetryableErrors:   60,
		RetryExhaustedErrors: 40,
		ByCollection: map[string]CollectionErrorStats{
			"users": {Total: 50, NonRetryable: 30, RetryExhausted: 20},
		},
		ByOperation: map[string]OperationErrorStats{
			"insert_one": {Total: 50, NonRetryable: 30, RetryExhausted: 20},
		},
		TopErrors: []ErrorCount{
			{Error: "error1", Count: 30},
		},
	}

	if snapshot.TotalErrors != 100 {
		t.Errorf("TotalErrors = %d, want 100", snapshot.TotalErrors)
	}
	if snapshot.NonRetryableErrors != 60 {
		t.Errorf("NonRetryableErrors = %d, want 60", snapshot.NonRetryableErrors)
	}
	if snapshot.RetryExhaustedErrors != 40 {
		t.Errorf("RetryExhaustedErrors = %d, want 40", snapshot.RetryExhaustedErrors)
	}
	if len(snapshot.ByCollection) != 1 {
		t.Errorf("ByCollection length = %d, want 1", len(snapshot.ByCollection))
	}
	if len(snapshot.ByOperation) != 1 {
		t.Errorf("ByOperation length = %d, want 1", len(snapshot.ByOperation))
	}
	if len(snapshot.TopErrors) != 1 {
		t.Errorf("TopErrors length = %d, want 1", len(snapshot.TopErrors))
	}
}
