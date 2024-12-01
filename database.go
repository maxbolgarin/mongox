package mongox

import (
	"context"
	"sync"

	"github.com/maxbolgarin/errm"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Database is a database client with open connection that creates collections and handles transactions.
type Database struct {
	database *mongo.Database

	colls map[string]*Collection
	mu    sync.RWMutex
}

// Database returns the underlying mongo database.
func (m *Database) Database() *mongo.Database {
	return m.database
}

// Collection returns a collection object by name.
// It will create a new collection if it doesn't exist after first query.
func (m *Database) Collection(name string) *Collection {
	m.mu.RLock()
	coll, ok := m.colls[name]
	m.mu.RUnlock()

	if ok {
		return coll
	}

	db := &Collection{
		coll: m.database.Collection(name),
	}

	m.mu.Lock()
	m.colls[name] = db
	m.mu.Unlock()

	return db
}

// WithTransaction executes a transaction.
// It will create a new session and execute a function inside a transaction.
// The fn callback may be run multiple times during WithTransaction due to retry attempts, so it must be idempotent.
func (m *Database) WithTransaction(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	session, err := m.database.Client().StartSession()
	if err != nil {
		return nil, errm.Wrap(err, "start session")
	}
	defer session.EndSession(ctx)

	// It commits the transaction.
	result, err := session.WithTransaction(ctx, fn)
	if err != nil {
		return nil, errm.Wrap(err, "transaction")
	}

	return result, nil
}
