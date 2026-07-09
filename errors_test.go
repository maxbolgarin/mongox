package mongox

import (
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestHandleMongoErrorWriteConcernOnly verifies that exceptions carrying only
// a write concern error are not swallowed into a nil error (regression test).
func TestHandleMongoErrorWriteConcernOnly(t *testing.T) {
	bwe := mongo.BulkWriteException{
		WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"},
	}
	got := HandleMongoError(bwe)
	if got == nil {
		t.Fatal("BulkWriteException with only WriteConcernError must not map to nil")
	}
	if !errors.Is(got, ErrWriteConcernFailed) {
		t.Fatalf("got %v, want ErrWriteConcernFailed", got)
	}

	we := mongo.WriteException{
		WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"},
	}
	got = HandleMongoError(we)
	if got == nil {
		t.Fatal("WriteException with only WriteConcernError must not map to nil")
	}
	if !errors.Is(got, ErrWriteConcernFailed) {
		t.Fatalf("got %v, want ErrWriteConcernFailed", got)
	}
}

// TestHandleMongoErrorEmptyExceptions verifies that write exceptions without
// any inner errors still surface as errors instead of nil.
func TestHandleMongoErrorEmptyExceptions(t *testing.T) {
	if got := HandleMongoError(mongo.WriteException{}); got == nil {
		t.Fatal("empty WriteException must not map to nil")
	}
	if got := HandleMongoError(mongo.BulkWriteException{}); got == nil {
		t.Fatal("empty BulkWriteException must not map to nil")
	}
}

func TestHandleMongoErrorWriteErrorsMapped(t *testing.T) {
	bwe := mongo.BulkWriteException{
		WriteErrors: []mongo.BulkWriteError{{
			WriteError: mongo.WriteError{Code: 11000, Message: "E11000 duplicate key error"},
		}},
	}
	got := HandleMongoError(bwe)
	if !errors.Is(got, ErrDuplicateKey) && !errors.Is(got, ErrDuplicate) {
		t.Fatalf("got %v, want duplicate key error", got)
	}
}
