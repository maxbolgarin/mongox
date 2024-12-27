package mongox

import (
	"context"
	"errors"
	"sync"

	"github.com/maxbolgarin/gorder"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DefaultAsyncRetries is the maximum number of retries for failed tasks in async mode.
const DefaultAsyncRetries = 10

// AsyncDatabase is a database client that handles operations asynchronously without waiting for them to complete.
// It is safe for concurrent use by multiple goroutines.
type AsyncDatabase struct {
	db    *Database
	queue *gorder.Gorder[string]
	log   gorder.Logger

	colls map[string]*AsyncCollection
	mu    sync.RWMutex
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

	coll = &AsyncCollection{
		coll:  m.db.Collection(name),
		queue: m.queue,
		log:   m.log,
	}

	m.mu.Lock()
	m.colls[name] = coll
	m.mu.Unlock()

	return coll
}

// WithTransaction executes a transaction asynchronously.
// It will create a new session and execute a function inside a transaction.
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
		return err
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

// AsyncCollection is a collection client that handles operations asynchronously without waiting for them to complete.
// It is safe for concurrent use by multiple goroutines.
// Tasks in different queues will be executed in parallel.
type AsyncCollection struct {
	coll  *Collection
	queue *gorder.Gorder[string]
	log   gorder.Logger
}

// Insert inserts a document or many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) Insert(queueKey, taskName string, records ...any) {
	ac.push(queueKey, taskName, "insert", func(ctx context.Context) error {
		_, err := ac.coll.Insert(ctx, records...)
		return err
	})
}

// InsertMany inserts many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
// Tasks in different queues will be executed in parallel.
func (ac *AsyncCollection) InsertMany(queueKey, taskName string, records []any) {
	ac.push(queueKey, taskName, "insert_many", func(ctx context.Context) error {
		_, err := ac.coll.InsertMany(ctx, records)
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

func (ac *AsyncCollection) push(queueKey, taskName, opName string, f gorder.TaskFunc) {
	if queueKey == "" {
		queueKey = ac.coll.coll.Name()
	}
	if taskName == "" {
		taskName = ac.coll.coll.Name() + "_" + opName
	}
	ac.queue.Push(queueKey, taskName, func(ctx context.Context) error {
		return ac.HandleRetryError(f(ctx), taskName)
	})
}

func (ac *AsyncCollection) HandleRetryError(err error, taskName string) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, ErrNotFound):
		// ErrNotFound is read error, it doesn't change state of the document and it can be throwed
		ac.log.Error("document not found", "error", err, "collection", ac.coll.coll.Name(), "task", taskName, "flow", "async")
		return nil

	case errors.Is(err, ErrDuplicate):
		// ErrDuplicate is a persistent error, there is no sense to retry it
		ac.log.Error("duplicate", "error", err, "collection", ac.coll.coll.Name(), "task", taskName, "flow", "async")
		return nil

	case errors.Is(err, ErrInvalidArgument) ||
		errors.Is(err, ErrBadValue) ||
		errors.Is(err, ErrIndexNotFound) ||
		errors.Is(err, ErrFailedToParse) ||
		errors.Is(err, ErrTypeMismatch) ||
		errors.Is(err, ErrIllegalOperation):
		// ErrInvalidArgument means error with using mongo interface
		// It is a persistent error and there is no sense to retry
		ac.log.Error("invalid argument", "error", err, "collection", ac.coll.coll.Name(), "task", taskName, "flow", "async")
		return nil

	default: // network, timeout, server and other errors should be retried
		return err
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

// Queue returns the queue key.
func (qc *QueueCollection) Queue() string {
	return qc.name
}

// Insert inserts a document or many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) Insert(records ...any) {
	qc.AsyncCollection.Insert(qc.name, "", records...)
}

// InsertMany inserts many documents into the collection asynchronously without waiting.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate, ErrInvalidArgument and some other errors.
func (qc *QueueCollection) InsertMany(records []any) {
	qc.AsyncCollection.InsertMany(qc.name, "", records)
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
