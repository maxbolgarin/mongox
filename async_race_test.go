package mongox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newOfflineClient returns a Client whose underlying mongo.Client is never
// connected. Collection/Database handles work without a live server.
func newOfflineClient(t *testing.T) *Client {
	t.Helper()
	mc, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:1"))
	if err != nil {
		t.Fatalf("create offline mongo client: %v", err)
	}
	t.Cleanup(func() { _ = mc.Disconnect(context.Background()) })
	return &Client{
		client: mc,
		dbs:    make(map[string]*Database),
		adbs:   make(map[string]*AsyncDatabase),
	}
}

// TestAsyncErrorHandlerAndStatsRace hammers WithErrorHandler / WithNoAsyncStats /
// ErrorStats concurrently with error reporting. Run with -race (regression test
// for unsynchronized errorHandler/stats access).
func TestAsyncErrorHandlerAndStatsRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := newOfflineClient(t)
	adb := client.AsyncDatabase(ctx, "racedb", 1, slog.Default())
	ac := adb.AsyncCollection("racecoll")

	stop := make(chan struct{})
	var mutator sync.WaitGroup
	mutator.Add(1)
	go func() {
		defer mutator.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			adb.WithErrorHandler(func(err *AsyncError) {})
			_ = adb.ErrorStats()
			if i%10 == 0 {
				adb.WithNoAsyncStats()
				adb.ResetErrorStats()
				adb.stats.Store(newAsyncErrorStats())
			}
			if i%25 == 0 {
				adb.WithErrorHandler(nil)
			}
		}
	}()

	var reporters sync.WaitGroup
	for range 4 {
		reporters.Add(1)
		go func() {
			defer reporters.Done()
			for range 500 {
				ac.reportError(errors.New("boom"), "task", "op", "queue")
				adb.reportRetryExhausted("queue", 3)
			}
		}()
	}

	reporters.Wait()
	close(stop)
	mutator.Wait()
}

// TestCachedGettersReturnSameInstance verifies that concurrent first calls to
// the cached getters all receive the same instance (regression test for
// check-then-act races; for AsyncDatabase a duplicate meant a leaked queue).
func TestCachedGettersReturnSameInstance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := newOfflineClient(t)

	const workers = 32

	t.Run("Client.Database", func(t *testing.T) {
		results := make([]*Database, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = client.Database("db")
			}()
		}
		wg.Wait()
		for i := range workers {
			if results[i] != results[0] {
				t.Fatalf("got different Database instances at %d", i)
			}
		}
	})

	t.Run("Client.AsyncDatabase", func(t *testing.T) {
		results := make([]*AsyncDatabase, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = client.AsyncDatabase(ctx, "adb", 1, slog.Default())
			}()
		}
		wg.Wait()
		for i := range workers {
			if results[i] != results[0] {
				t.Fatalf("got different AsyncDatabase instances at %d", i)
			}
		}
	})

	t.Run("Database.Collection", func(t *testing.T) {
		db := client.Database("db")
		results := make([]*Collection, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = db.Collection("coll")
			}()
		}
		wg.Wait()
		for i := range workers {
			if results[i] != results[0] {
				t.Fatalf("got different Collection instances at %d", i)
			}
		}
	})

	t.Run("AsyncDatabase.AsyncCollection", func(t *testing.T) {
		adb := client.AsyncDatabase(ctx, "adb", 1, slog.Default())
		results := make([]*AsyncCollection, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = adb.AsyncCollection("coll")
			}()
		}
		wg.Wait()
		for i := range workers {
			if results[i] != results[0] {
				t.Fatalf("got different AsyncCollection instances at %d", i)
			}
		}
	})
}
