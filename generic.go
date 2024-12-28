package mongox

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Name returns the name of the collection.
func Name(coll *Collection) string {
	return coll.Name()
}

// CreateIndex creates an index for a collection with the given field names.
// Field names are required and must be unique.
func CreateIndex(ctx context.Context, coll *Collection, isUnique bool, fieldNames ...string) error {
	return coll.CreateIndex(ctx, isUnique, fieldNames...)
}

// CreateTextIndex creates a text index for a collection with the given field names and language code.
// You should create a text index to use text search. Field names are required and must be unique.
// If the language code is not provided, "en" will be used by default.
func CreateTextIndex(ctx context.Context, coll *Collection, languageCode string, fieldNames ...string) error {
	return coll.CreateTextIndex(ctx, languageCode, fieldNames...)
}

// FindOne finds a one document in the collection using filter.
// It returns ErrNotFound if NO document is found.
// Limit and AllowDiskUse options are no-op.
func FindOne[T any](ctx context.Context, coll *Collection, filter M, opts ...FindOptions) (T, error) {
	var result T
	if err := coll.FindOne(ctx, &result, filter, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// Find finds many documents in the collection using filter.
// It does NOT return any error if no document is found.
func Find[T any](ctx context.Context, coll *Collection, filter M, opts ...FindOptions) ([]T, error) {
	var result []T
	if err := coll.Find(ctx, &result, filter, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// FindAll finds all documents in the collection.
// It does NOT return any error if no document is found.
func FindAll[T any](ctx context.Context, coll *Collection, opts ...FindOptions) ([]T, error) {
	var result []T
	if err := coll.FindAll(ctx, &result, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// Count counts the number of documents in the collection using filter.
// Nil filter means count all documents.
func Count(ctx context.Context, coll *Collection, filter M) (int64, error) {
	return coll.Count(ctx, filter)
}

// Distinct finds distinct values for the specified field in the collection.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
func Distinct[T any](ctx context.Context, coll *Collection, field string, filter M) ([]T, error) {
	var result []T
	if err := coll.Distinct(ctx, &result, field, filter); err != nil {
		return result, err
	}
	return result, nil
}

// Insert inserts a document(s) into the collection
// It returns IDs of the inserted documents.
// Internally InsertMany uses bulk write.
func Insert(ctx context.Context, coll *Collection, record ...any) ([]bson.ObjectID, error) {
	return coll.Insert(ctx, record...)
}

// InsertMany inserts many documents into the collection.
// It returns IDs of the inserted documents.
// Internally InsertMany uses bulk write.
func InsertMany(ctx context.Context, coll *Collection, records []any) ([]bson.ObjectID, error) {
	return coll.InsertMany(ctx, records)
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist.
// It returns ID of the inserted document.
// If existing document is updated (no new inserted), it returns nil ID and nil error.
// If no document is updated, it returns nil ID and ErrNotFound.
func Upsert(ctx context.Context, coll *Collection, record any, filter M) (*bson.ObjectID, error) {
	return coll.Upsert(ctx, record, filter)
}

// ReplaceOne replaces a document in the collection.
// It returns ErrNotFound if no document is updated.
func ReplaceOne(ctx context.Context, coll *Collection, record any, filter M) error {
	return coll.ReplaceOne(ctx, record, filter)
}

// SetFields sets fields in a document in the collection using updates map.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}.
// It returns ErrNotFound if no document is updated.
func SetFields(ctx context.Context, coll *Collection, filter M, update map[string]any) error {
	return coll.SetFields(ctx, filter, update)
}

// UpdateOne updates a document in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It returns ErrNotFound if no document is updated.
func UpdateOne(ctx context.Context, coll *Collection, filter, update M) error {
	return coll.UpdateOne(ctx, filter, update)
}

// UpdateMany updates multi documents in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It returns number of updated documents.
// It returns ErrNotFound if no document is updated.
func UpdateMany(ctx context.Context, coll *Collection, filter, update M) (int, error) {
	return coll.UpdateMany(ctx, filter, update)
}

// UpdateOneFromDiff sets fields in a document in the collection using diff structure.
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
func UpdateOneFromDiff(ctx context.Context, coll *Collection, filter M, diff any) error {
	return coll.UpdateOneFromDiff(ctx, filter, diff)
}

// DeleteFields deletes fields in a document in the collection.
// It returns ErrNotFound if no document is updated.
func DeleteFields(ctx context.Context, coll *Collection, filter M, fields ...string) error {
	return coll.DeleteFields(ctx, filter, fields...)
}

// DeleteOne deletes a document in the collection based on the filter.
// It returns ErrNotFound if no document is deleted.
func DeleteOne(ctx context.Context, coll *Collection, filter M) error {
	return coll.DeleteOne(ctx, filter)
}

// DeleteMany deletes documents in the collection based on the filter.
// It returns number of deleted documents.
// It returns ErrNotFound if no document is deleted.
func DeleteMany(ctx context.Context, coll *Collection, filter M) (int, error) {
	return coll.DeleteMany(ctx, filter)
}

// BulkWrite executes bulk write operations in the collection.
// Use [BulkBuilder] to create models for bulk write operations.
// IsOrdered==true means that all operations are executed in the order they are added to the [BulkBuilder]
// and if any of them fails, the whole operation fails. Error is not returning.
// IsOrdered==false means that all operations are executed in parallel and if any of them fails,
// the whole operation continues. Error is not returning.
// It returns ErrNotFound if no document is matched/inserted/updated/deleted.
func BulkWrite(ctx context.Context, coll *Collection, models []mongo.WriteModel, isOrdered bool) (mongo.BulkWriteResult, error) {
	return coll.BulkWrite(ctx, models, isOrdered)
}
