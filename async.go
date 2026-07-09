package mongox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxbolgarin/gorder"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DefaultAsyncRetries is the maximum number of retries for failed tasks in async mode.
const DefaultAsyncRetries = 10

// AsyncError contains information about an async operation error.
type AsyncError struct {
	// Err is the underlying error.
	Err error
	// Collection is the name of the collection where the error occurred.
	Collection string
	// Operation is the name of the operation (insert_one, update_one, etc.).
	Operation string
	// TaskName is the user-provided task name.
	TaskName string
	// QueueKey is the queue key where the task was executed.
	QueueKey string
	// Timestamp is when the error occurred.
	Timestamp time.Time
	// RetryCount is the number of retries attempted (for retry-exhausted errors).
	RetryCount int
	// IsNotRetryable indicates if the error is not retryable.
	IsNotRetryable bool
}

// Error implements the error interface.
func (e *AsyncError) Error() string {
	return fmt.Sprintf("async error in %s.%s [%s]: %v", e.Collection, e.Operation, e.TaskName, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As compatibility.
func (e *AsyncError) Unwrap() error {
	return e.Err
}

// AsyncErrorStats is a point-in-time snapshot of error statistics.
type AsyncErrorStats struct {
	// TotalErrors is the total number of errors recorded.
	TotalErrors int64
	// NonRetryableErrors is the count of non-retryable errors.
	NonRetryableErrors int64
	// RetryExhaustedErrors is the count of retry-exhausted errors.
	RetryExhaustedErrors int64
	// ByCollection contains error statistics grouped by collection name.
	ByCollection map[string]CollectionErrorStats
	// ByOperation contains error statistics grouped by operation type.
	ByOperation map[string]OperationErrorStats
	// TopErrors contains the top 10 most frequent error messages.
	TopErrors []ErrorCount
}

// CollectionErrorStats contains error statistics for a collection.
type CollectionErrorStats struct {
	Total          int64
	NonRetryable   int64
	RetryExhausted int64
}

// OperationErrorStats contains error statistics for an operation type.
type OperationErrorStats struct {
	Total          int64
	NonRetryable   int64
	RetryExhausted int64
}

// ErrorCount represents an error message and its occurrence count.
type ErrorCount struct {
	Error string
	Count int64
}

// AsyncErrorHandler is a callback function for handling async errors.
// It should be non-blocking and thread-safe.
type AsyncErrorHandler func(err *AsyncError)

// AsyncDatabase is a database client that handles operations asynchronously without waiting for them to complete.
// It is safe for concurrent use by multiple goroutines.
type AsyncDatabase struct {
	db    *Database
	queue *gorder.Gorder[string]
	log   gorder.Logger

	colls map[string]*AsyncCollection
	mu    sync.RWMutex

	// errorHandler and stats are shared with every AsyncCollection and are
	// read from queue worker goroutines, so they must be accessed atomically.
	errorHandler atomic.Pointer[AsyncErrorHandler]
	stats        atomic.Pointer[asyncErrorStats]
}

// Database returns the underlying Database.
func (m *AsyncDatabase) Database() *Database {
	return m.db
}

// AsyncCollection returns an async collection object by name.
// It will create a new collection if it doesn't exist after first query.
func (m *AsyncDatabase) AsyncCollection(name string) *AsyncCollection {
	m.mu.RLock()
	coll, ok := m.colls[name]
	m.mu.RUnlock()

	if ok {
		return coll
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock to avoid duplicate creation.
	if coll, ok = m.colls[name]; ok {
		return coll
	}

	coll = &AsyncCollection{
		coll:         m.db.Collection(name),
		queue:        m.queue,
		log:          m.log,
		errorHandler: &m.errorHandler,
		stats:        &m.stats,
	}
	m.colls[name] = coll

	return coll
}

// WithTransaction executes a transaction asynchronously.
// It will create a new session and execute a function inside a transaction.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Warning! Transactions in MongoDB is available only for replica sets or Sharded Clusters, not for standalone servers.
func (m *AsyncDatabase) WithTransaction(queueKey, taskName string, fn func(ctx context.Context) error) {
	if queueKey == "" {
		queueKey = m.db.db.Name()
	}
	if taskName == "" {
		taskName = m.db.db.Name() + "_transaction"
	}
	m.queue.Push(queueKey, taskName, func(ctx context.Context) error {
		_, err := m.db.WithTransaction(ctx, func(ctx context.Context) (any, error) {
			return nil, fn(ctx)
		})
		return filterAsyncRetryError(err, m.log, m.db.db.Name(), taskName, func(err error) {
			m.reportError(err, "", "transaction", taskName, queueKey)
		})
	})
}

// WithTask adds a function to execute it asynchronously.
// It won't handle errors like in collection method and will retry function in case of returning any error.
// If queue is empty, it will use the database name.
func (m *AsyncDatabase) WithTask(queueKey, taskName string, fn func(ctx context.Context) error) {
	if queueKey == "" {
		queueKey = m.db.db.Name()
	}
	if taskName == "" {
		taskName = m.db.db.Name() + "_task"
	}
	m.queue.Push(queueKey, taskName, func(ctx context.Context) error {
		return fn(ctx)
	})
}

// WithErrorHandler sets a callback function for handling async errors.
// The handler is called synchronously but should be non-blocking.
// Pass nil to remove the current handler.
// It is thread-safe and can be called at any time.
func (m *AsyncDatabase) WithErrorHandler(handler AsyncErrorHandler) *AsyncDatabase {
	if handler == nil {
		m.errorHandler.Store(nil)
	} else {
		m.errorHandler.Store(&handler)
	}
	return m
}

// WithNoAsyncStats disables error statistics collection.
// It is thread-safe and can be called at any time.
func (m *AsyncDatabase) WithNoAsyncStats() *AsyncDatabase {
	m.stats.Store(nil)
	return m
}

// ErrorStats returns a point-in-time snapshot of error statistics.
// The snapshot is a copy and can be safely used without synchronization.
func (m *AsyncDatabase) ErrorStats() AsyncErrorStats {
	stats := m.stats.Load()
	if stats == nil {
		return AsyncErrorStats{}
	}
	return stats.snapshot()
}

// ResetErrorStats clears all error statistics.
func (m *AsyncDatabase) ResetErrorStats() {
	if stats := m.stats.Load(); stats != nil {
		stats.reset()
	}
}

// StartRetryExhaustedMonitor starts a background goroutine that periodically checks for retry-exhausted errors.
// It monitors the gorder queue for broken queues (tasks that exhausted all retries) and reports them
// through the error handler. The monitor runs until the context is canceled.
// The interval specifies how often to check for retry-exhausted errors.
func (m *AsyncDatabase) StartRetryExhaustedMonitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	check := func(seen map[string]int) {
		broken := m.queue.BrokenQueues()
		// Reset/prune seen for queues that recovered
		for k := range seen {
			if _, ok := broken[k]; !ok {
				delete(seen, k)
			}
		}
		// Report on increases; reset baseline if counter dropped
		for queueKey, retries := range broken {
			last, ok := seen[queueKey]
			if !ok || retries > last {
				m.reportRetryExhausted(queueKey, retries)
			}
			if !ok || retries < last {
				// reset baseline after recovery or counter reset
				seen[queueKey] = retries
			} else if retries > last {
				seen[queueKey] = retries
			}
		}
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		seen := make(map[string]int)

		// Immediate scan
		check(seen)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				check(seen)
			}
		}
	}()
}

// reportError sends a non-retryable error to the configured handler and records statistics.
func (m *AsyncDatabase) reportError(err error, collection, opName, taskName, queueKey string) {
	asyncErr := &AsyncError{
		Err:            err,
		Collection:     collection,
		Operation:      opName,
		TaskName:       taskName,
		QueueKey:       queueKey,
		Timestamp:      time.Now(),
		RetryCount:     0,
		IsNotRetryable: true,
	}

	// Record statistics
	if stats := m.stats.Load(); stats != nil {
		stats.record(asyncErr)
	}

	// Call user handler
	if handler := m.errorHandler.Load(); handler != nil && *handler != nil {
		(*handler)(asyncErr)
	}
}

// reportRetryExhausted reports a retry-exhausted error.
func (m *AsyncDatabase) reportRetryExhausted(queueKey string, retryCount int) {
	asyncErr := &AsyncError{
		Err:            fmt.Errorf("task exhausted all %d retries", retryCount),
		IsNotRetryable: false,
		Collection:     "",
		Operation:      "unknown",
		TaskName:       queueKey,
		QueueKey:       queueKey,
		Timestamp:      time.Now(),
		RetryCount:     retryCount,
	}

	// Record statistics
	if stats := m.stats.Load(); stats != nil {
		stats.record(asyncErr)
	}

	// Call user handler
	if handler := m.errorHandler.Load(); handler != nil && *handler != nil {
		(*handler)(asyncErr)
	}
}

// AsyncCollection is a collection client that handles operations asynchronously without waiting for them to complete.
// It is safe for concurrent use by multiple goroutines.
// Tasks in different queues will be executed in parallel.
type AsyncCollection struct {
	coll  *Collection
	queue *gorder.Gorder[string]
	log   gorder.Logger

	errorHandler *atomic.Pointer[AsyncErrorHandler] // Shared with the database
	stats        *atomic.Pointer[asyncErrorStats]   // Shared with the database
}

// Name returns the name of the collection.
func (ac *AsyncCollection) Name() string {
	return ac.coll.Name()
}

// Collection returns an original mongo.Collection object.
func (ac *AsyncCollection) Collection() *mongo.Collection {
	return ac.coll.Collection()
}

// InsertOne inserts a document into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// It returns ErrInternal if no inserted ID is returned.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (ac *AsyncCollection) InsertOne(queueKey, taskName string, record any, isStrictID ...bool) {
	ac.push(queueKey, taskName, "insert_one", func(ctx context.Context) error {
		_, err := ac.coll.InsertOne(ctx, record, isStrictID...)
		return err
	})
}

// Insert inserts a document or many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
// It NOT returns an error if inserted IDs are not ObjectID, so it is NOT strict.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (ac *AsyncCollection) Insert(queueKey, taskName string, records ...any) {
	ac.push(queueKey, taskName, "insert", func(ctx context.Context) error {
		_, err := ac.coll.Insert(ctx, records...)
		return err
	})
}

// InsertStrict inserts a document or many documents into the collection.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
// It returns IDs of the inserted documents. Internally InsertMany uses bulk write.
// It returns an error if inserted IDs are not ObjectID.
func (ac *AsyncCollection) InsertStrict(queueKey, taskName string, records ...any) {
	ac.push(queueKey, taskName, "insert_strict", func(ctx context.Context) error {
		_, err := ac.coll.InsertStrict(ctx, records...)
		return err
	})
}

// InsertMany inserts many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// If isStrictID is false and if inserted ID is not an ObjectID, it will be returned as empty bson.ObjectID.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (ac *AsyncCollection) InsertMany(queueKey, taskName string, records []any, isStrictID ...bool) {
	ac.push(queueKey, taskName, "insert_many", func(ctx context.Context) error {
		_, err := ac.coll.InsertMany(ctx, records, isStrictID...)
		return err
	})
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) Upsert(queueKey, taskName string, record any, filter M) {
	ac.push(queueKey, taskName, "upsert", func(ctx context.Context) error {
		_, err := ac.coll.Upsert(ctx, record, filter)
		return err
	})
}

// ReplaceOne replaces a document in the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) ReplaceOne(queueKey, taskName string, record any, filter M) {
	ac.push(queueKey, taskName, "replace", func(ctx context.Context) error {
		return ac.coll.ReplaceOne(ctx, record, filter)
	})
}

// SetFields sets fields in a document in the collection asynchronously without waiting.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) SetFields(queueKey, taskName string, filter, update M) {
	ac.push(queueKey, taskName, "set_fields", func(ctx context.Context) error {
		return ac.coll.SetFields(ctx, filter, update)
	})
}

// UpdateOne updates a document in the collection asynchronously without waiting for it to complete.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) UpdateOne(queueKey, taskName string, filter, update M) {
	ac.push(queueKey, taskName, "update_one", func(ctx context.Context) error {
		return ac.coll.UpdateOne(ctx, filter, update)
	})
}

// UpdateMany updates multi documents in the collection asynchronously without waiting for them to complete.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound,  ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) UpdateMany(queueKey, taskName string, filter, update M) {
	ac.push(queueKey, taskName, "update_many", func(ctx context.Context) error {
		_, err := ac.coll.UpdateMany(ctx, filter, update)
		return err
	})
}

// UpdateOneFromDiff sets fields in a document in the collection using diff structure asynchronously without waiting for it to complete.
// Diff structure is a map of pointers to field names with their new values.
// E.g. if you have structure:
//
//	type MyStruct struct {name string, index int}
//
// Diff structure will be:
//
//	type MyStructDiff struct {name *string, index *int}
//
// It returns ErrNotFound if no document is updated.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) UpdateOneFromDiff(queueKey, taskName string, filter M, diff any) {
	ac.push(queueKey, taskName, "update_from_diff", func(ctx context.Context) error {
		return ac.coll.UpdateOneFromDiff(ctx, filter, diff)
	})
}

// DeleteFields deletes fields in a document in the collection asynchronously without waiting for it to complete.
// For example: [key1, key2] becomes {$unset: {key1: "", key2: ""}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) DeleteFields(queueKey, taskName string, filter M, fields ...string) {
	ac.push(queueKey, taskName, "delete_fields", func(ctx context.Context) error {
		return ac.coll.DeleteFields(ctx, filter, fields...)
	})
}

// DeleteOne deletes a document in the collection asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) DeleteOne(queueKey, taskName string, filter M) {
	ac.push(queueKey, taskName, "delete_one", func(ctx context.Context) error {
		return ac.coll.DeleteOne(ctx, filter)
	})
}

// DeleteMany deletes multi documents in the collection asynchronously without waiting for them to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) DeleteMany(queueKey, taskName string, filter M) {
	ac.push(queueKey, taskName, "delete_many", func(ctx context.Context) error {
		_, err := ac.coll.DeleteMany(ctx, filter)
		return err
	})
}

// BulkWrite executes bulk write operations in the collection asynchronously without waiting for them to complete.
// Use [BulkBuilder] to create models for bulk write operations.
// IsOrdered==true means that all operations are executed in the order they are added to the [BulkBuilder]
// and if any of them fails, the whole operation fails.
// IsOrdered==false means that all operations are executed in parallel and if any of them fails,
// the whole operation continues.
func (ac *AsyncCollection) BulkWrite(queueKey, taskName string, models []mongo.WriteModel, isOrdered bool) {
	ac.push(queueKey, taskName, "bulk_write", func(ctx context.Context) error {
		_, err := ac.coll.BulkWrite(ctx, models, isOrdered)
		return err
	})
}

// QueuesLength returns number of tasks for each queue.
func (ac *AsyncCollection) QueuesLength() map[string]int {
	out := make(map[string]int, len(ac.queue.Stat()))
	for k, v := range ac.queue.Stat() {
		out[k] = v.Length
	}
	return out
}

// QueueLength returns number of tasks for a given queue.
func (ac *AsyncCollection) QueueLength(queueKey string) int {
	return ac.QueuesLength()[queueKey]
}

func (ac *AsyncCollection) push(queueKey, taskName, opName string, f gorder.TaskFunc) {
	if queueKey == "" {
		queueKey = ac.coll.coll.Name()
	}
	if taskName == "" {
		taskName = ac.coll.coll.Name() + "_" + opName
	}
	ac.queue.Push(queueKey, taskName, func(ctx context.Context) error {
		return ac.handleRetryError(f(ctx), taskName, opName, queueKey)
	})
}

// handleRetryError processes errors from async operations, determining which should be retried.
// Non-retryable errors are logged and reported to the error handler but not retried.
// Retryable errors (network, timeout, server errors) are returned for retry by gorder.
func (ac *AsyncCollection) handleRetryError(err error, taskName, opName, queueKey string) error {
	return filterAsyncRetryError(err, ac.log, ac.coll.coll.Name(), taskName, func(err error) {
		ac.reportError(err, taskName, opName, queueKey)
	})
}

// filterAsyncRetryError processes errors from async operations, determining which should be retried.
// Non-retryable errors are logged and passed to report, then swallowed (nil is returned).
// Retryable errors (network, timeout, server errors) are returned for retry by gorder.
func filterAsyncRetryError(err error, log gorder.Logger, scope, taskName string, report func(error)) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, ErrNotFound):
		// ErrNotFound is read error, it doesn't change state of the document and it can be throwed
		log.Error("document not found", "error", err, "collection", scope, "task", taskName, "flow", "async")
		report(err)
		return nil

	case errors.Is(err, ErrDuplicate):
		// ErrDuplicate is a persistent error, there is no sense to retry it
		log.Error("duplicate", "error", err, "collection", scope, "task", taskName, "flow", "async")
		report(err)
		return nil

	case errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrBadValue) ||
		errors.Is(err, ErrIndexNotFound) ||
		errors.Is(err, ErrFailedToParse) ||
		errors.Is(err, ErrTypeMismatch) ||
		errors.Is(err, ErrIllegalOperation):
		// ErrInvalidArgument means error with using mongo interface
		// It is a persistent error and there is no sense to retry
		log.Error("invalid argument", "error", err, "collection", scope, "task", taskName, "flow", "async")
		report(err)
		return nil

	default: // network, timeout, server and other errors should be retried
		return err
	}
}

// reportError sends an error to the configured handler and records statistics.
func (ac *AsyncCollection) reportError(err error, taskName, opName, queueKey string) {
	asyncErr := &AsyncError{
		Err:            err,
		Collection:     ac.coll.coll.Name(),
		Operation:      opName,
		TaskName:       taskName,
		QueueKey:       queueKey,
		Timestamp:      time.Now(),
		RetryCount:     0,
		IsNotRetryable: true,
	}

	// Record statistics
	if stats := ac.stats.Load(); stats != nil {
		stats.record(asyncErr)
	}

	// Call user handler
	if handler := ac.errorHandler.Load(); handler != nil && *handler != nil {
		(*handler)(asyncErr)
	}
}

// QueueCollection is a async collection with predefined queue key.
type QueueCollection struct {
	*AsyncCollection
	name string
}

// QueueCollection returns a new QueueCollection.
func (qc *AsyncCollection) QueueCollection(name string) *QueueCollection {
	return &QueueCollection{AsyncCollection: qc, name: name}
}

// Name returns the name of the collection.
func (qc *QueueCollection) Name() string {
	return qc.AsyncCollection.Name()
}

// Collection returns an original mongo.Collection object.
func (qc *QueueCollection) Collection() *mongo.Collection {
	return qc.AsyncCollection.Collection()
}

// Queue returns the queue key.
func (qc *QueueCollection) Queue() string {
	return qc.name
}

// InsertOne inserts a document into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// It returns ErrInternal if no inserted ID is returned.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (qc *QueueCollection) InsertOne(record any, isStrictID ...bool) {
	qc.AsyncCollection.InsertOne(qc.name, "", record, isStrictID...)
}

// Insert inserts a document or many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// It NOT returns an error if inserted IDs are not ObjectID, so it is NOT strict.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (qc *QueueCollection) Insert(records ...any) {
	qc.AsyncCollection.Insert(qc.name, "", records...)
}

// InsertStrict inserts a document or many documents into the collection.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// It returns IDs of the inserted documents. Internally InsertMany uses bulk write.
// It returns an error if inserted IDs are not ObjectID.
func (qc *QueueCollection) InsertStrict(records ...any) {
	qc.AsyncCollection.InsertStrict(qc.name, "", records...)
}

// InsertMany inserts many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// If isStrictID is false and if inserted ID is not an ObjectID, it will be returned as empty bson.ObjectID.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (qc *QueueCollection) InsertMany(records []any, isStrictID ...bool) {
	qc.AsyncCollection.InsertMany(qc.name, "", records, isStrictID...)
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) Upsert(record any, filter M) {
	qc.AsyncCollection.Upsert(qc.name, "", record, filter)
}

// ReplaceOne replaces a document in the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) ReplaceOne(record any, filter M) {
	qc.AsyncCollection.ReplaceOne(qc.name, "", record, filter)
}

// SetFields sets fields in a document in the collection asynchronously without waiting.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) SetFields(filter, update M) {
	qc.AsyncCollection.SetFields(qc.name, "", filter, update)
}

// UpdateOne updates a document in the collection asynchronously without waiting for it to complete.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) UpdateOne(filter, update M) {
	qc.AsyncCollection.UpdateOne(qc.name, "", filter, update)
}

// UpdateMany updates multi documents in the collection asynchronously without waiting for them to complete.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound,  ErrInvalidArgument and some other errors.
func (qc *QueueCollection) UpdateMany(filter, update M) {
	qc.AsyncCollection.UpdateMany(qc.name, "", filter, update)
}

// UpdateOneFromDiff sets fields in a document in the collection using diff structure asynchronously without waiting for it to complete.
// Diff structure is a map of pointers to field names with their new values.
// E.g. if you have structure:
//
//	type MyStruct struct {name string, index int}
//
// Diff structure will be:
//
//	type MyStructDiff struct {name *string, index *int}
//
// It returns ErrNotFound if no document is updated.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) UpdateOneFromDiff(filter M, diff any) {
	qc.AsyncCollection.UpdateOneFromDiff(qc.name, "", filter, diff)
}

// DeleteFields deletes fields in a document in the collection asynchronously without waiting for it to complete.
// For example: [key1, key2] becomes {$unset: {key1: "", key2: ""}}.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) DeleteFields(filter M, fields ...string) {
	qc.AsyncCollection.DeleteFields(qc.name, "", filter, fields...)
}

// DeleteOne deletes a document in the collection asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) DeleteOne(filter M) {
	qc.AsyncCollection.DeleteOne(qc.name, "", filter)
}

// DeleteMany deletes multi documents in the collection asynchronously without waiting for them to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) DeleteMany(filter M) {
	qc.AsyncCollection.DeleteMany(qc.name, "", filter)
}

// BulkWrite executes bulk write operations in the collection asynchronously without waiting for them to complete.
// Use [BulkBuilder] to create models for bulk write operations.
// IsOrdered==true means that all operations are executed in the order they are added to the [BulkBuilder]
// and if any of them fails, the whole operation fails.
// IsOrdered==false means that all operations are executed in parallel and if any of them fails,
// the whole operation continues.
func (qc *QueueCollection) BulkWrite(models []mongo.WriteModel, isOrdered bool) {
	qc.AsyncCollection.BulkWrite(qc.name, "", models, isOrdered)
}

// QueuesLength returns number of tasks for each queue.
func (qc *QueueCollection) QueuesLength() map[string]int {
	return qc.AsyncCollection.QueuesLength()
}

// QueueLength returns number of tasks for a given queue.
func (qc *QueueCollection) QueueLength(queueKey string) int {
	return qc.AsyncCollection.QueueLength(queueKey)
}

// collectionErrorStats holds error statistics for a single collection.
type collectionErrorStats struct {
	total          int64
	nonRetryable   int64
	retryExhausted int64
}

// operationErrorStats holds error statistics for a single operation type.
type operationErrorStats struct {
	total          int64
	nonRetryable   int64
	retryExhausted int64
}

// asyncErrorStats tracks error statistics in a thread-safe manner.
type asyncErrorStats struct {
	mu                   sync.RWMutex
	totalErrors          int64
	nonRetryableErrors   int64
	retryExhaustedErrors int64
	byCollection         map[string]*collectionErrorStats
	byOperation          map[string]*operationErrorStats
	byErrorType          map[string]int64 // error message -> count
}

// newAsyncErrorStats creates a new AsyncErrorStats instance.
func newAsyncErrorStats() *asyncErrorStats {
	return &asyncErrorStats{
		byCollection: make(map[string]*collectionErrorStats),
		byOperation:  make(map[string]*operationErrorStats),
		byErrorType:  make(map[string]int64),
	}
}

// Record adds an error to statistics.
func (s *asyncErrorStats) record(err *AsyncError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalErrors++
	if err.IsNotRetryable {
		s.nonRetryableErrors++
	} else {
		s.retryExhaustedErrors++
	}

	// Collection stats
	cs, ok := s.byCollection[err.Collection]
	if !ok {
		cs = &collectionErrorStats{}
		s.byCollection[err.Collection] = cs
	}
	cs.total++
	if err.IsNotRetryable {
		cs.nonRetryable++
	} else {
		cs.retryExhausted++
	}

	// Operation stats
	os, ok := s.byOperation[err.Operation]
	if !ok {
		os = &operationErrorStats{}
		s.byOperation[err.Operation] = os
	}
	os.total++
	if err.IsNotRetryable {
		os.nonRetryable++
	} else {
		os.retryExhausted++
	}

	// Error type counts
	errKey := err.Err.Error()
	s.byErrorType[errKey]++
}

// Snapshot returns a point-in-time snapshot of error statistics.
func (s *asyncErrorStats) snapshot() AsyncErrorStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := AsyncErrorStats{
		TotalErrors:          s.totalErrors,
		NonRetryableErrors:   s.nonRetryableErrors,
		RetryExhaustedErrors: s.retryExhaustedErrors,
		ByCollection:         make(map[string]CollectionErrorStats),
		ByOperation:          make(map[string]OperationErrorStats),
	}

	for k, v := range s.byCollection {
		snap.ByCollection[k] = CollectionErrorStats{
			Total:          v.total,
			NonRetryable:   v.nonRetryable,
			RetryExhausted: v.retryExhausted,
		}
	}

	for k, v := range s.byOperation {
		snap.ByOperation[k] = OperationErrorStats{
			Total:          v.total,
			NonRetryable:   v.nonRetryable,
			RetryExhausted: v.retryExhausted,
		}
	}

	// Top 10 errors
	type errPair struct {
		err   string
		count int64
	}
	pairs := make([]errPair, 0, len(s.byErrorType))
	for k, v := range s.byErrorType {
		pairs = append(pairs, errPair{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].count > pairs[j].count
	})

	top := 10
	if len(pairs) < top {
		top = len(pairs)
	}
	snap.TopErrors = make([]ErrorCount, top)
	for i := 0; i < top; i++ {
		snap.TopErrors[i] = ErrorCount{Error: pairs[i].err, Count: pairs[i].count}
	}

	return snap
}

// Reset clears all statistics.
func (s *asyncErrorStats) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalErrors = 0
	s.nonRetryableErrors = 0
	s.retryExhaustedErrors = 0
	s.byCollection = make(map[string]*collectionErrorStats)
	s.byOperation = make(map[string]*operationErrorStats)
	s.byErrorType = make(map[string]int64)
}
