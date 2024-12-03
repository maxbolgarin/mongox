package mongox

import (
	"context"
	"errors"
	"sync"

	"github.com/maxbolgarin/gorder"
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
// It won't handle errors like in collection method and will retry function in case of returning any error,
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
type AsyncCollection struct {
	coll  *Collection
	queue *gorder.Gorder[string]
	log   gorder.Logger
}

// Insert inserts a document(s) into the collection asynchronously without waiting for them to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) Insert(queueKey, taskName string, records ...any) {
	ac.push(queueKey, taskName, "insert", func(ctx context.Context) error {
		return ac.coll.Insert(ctx, records...)
	})
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) Upsert(queueKey, taskName string, record any, filter M) {
	ac.push(queueKey, taskName, "upsert", func(ctx context.Context) error {
		return ac.coll.Upsert(ctx, record, filter)
	})
}

// SetFields sets fields in a document in the collection asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) SetFields(queueKey, taskName string, filter M, update map[string]any) {
	ac.push(queueKey, taskName, "set_fields", func(ctx context.Context) error {
		return ac.coll.SetFields(ctx, filter, update)
	})
}

// UpdateOne updates a document in the collection asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) UpdateOne(queueKey, taskName string, filter, update M) {
	ac.push(queueKey, taskName, "update_one", func(ctx context.Context) error {
		return ac.coll.UpdateOne(ctx, filter, update)
	})
}

// UpdateMany updates multi documents in the collection asynchronously without waiting for them to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) UpdateMany(queueKey, taskName string, filter, update M) {
	ac.push(queueKey, taskName, "update_many", func(ctx context.Context) error {
		return ac.coll.UpdateMany(ctx, filter, update)
	})
}

// UpdateFromDiff sets fields in a document in the collection using diff structure asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) UpdateFromDiff(queueKey, taskName string, filter M, diff any) {
	ac.push(queueKey, taskName, "update_from_diff", func(ctx context.Context) error {
		return ac.coll.UpdateFromDiff(ctx, filter, diff)
	})
}

// DeleteOne deletes a document in the collection asynchronously without waiting for it to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) DeleteOne(queueKey, taskName string, filter M) {
	ac.push(queueKey, taskName, "delete_one", func(ctx context.Context) error {
		return ac.coll.DeleteOne(ctx, filter)
	})
}

// DeleteMany deletes multi documents in the collection asynchronously without waiting for them to complete.
// It start retrying in case of error for DefaultAsyncRetries times.
// It filters errors and won't retry in case of ErrNotFound, ErrDuplicate and ErrInvalidArgument.
func (ac *AsyncCollection) DeleteMany(queueKey, taskName string, filter M) {
	ac.push(queueKey, taskName, "delete_many", func(ctx context.Context) error {
		return ac.coll.DeleteMany(ctx, filter)
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
		return ac.handleRetryError(f(ctx))
	})
}

func (ac *AsyncCollection) handleRetryError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, ErrNotFound):
		// ErrNotFound is read error, it doesn't change state of the document and it can be throw
		ac.log.Error("document not found", "error", err, "collection", ac.coll.coll.Name(), "flow", "async")
		return nil

	case errors.Is(err, ErrDuplicate):
		// ErrDuplicate is a persistent error, there is no sense to retry it
		ac.log.Error("duplicate key error", "error", err, "collection", ac.coll.coll.Name(), "flow", "async")
		return nil

	case errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrBadValue) || errors.Is(err, ErrIndexNotFound):
		// ErrInvalidArgument means error with using mongo interface
		// It is a persistent error and there is no sense to retry
		ac.log.Error("invalid argument error", "error", err, "collection", ac.coll.coll.Name(), "flow", "async")
		return nil

	default: // network, timeout, server and other errors should be retried
		return err
	}
}
