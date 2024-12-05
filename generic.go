package mongox

import (
	"context"
)

// CreateIndex creates an index for a collection with the given field names.
func CreateIndex(ctx context.Context, coll *Collection, isUnique bool, fieldNames ...string) error {
	return coll.CreateIndex(ctx, isUnique, fieldNames...)
}

// CreateTextIndex creates a text index for a collection with the given field names and language code.
func CreateTextIndex(ctx context.Context, coll *Collection, languageCode string, fieldNames ...string) error {
	return coll.CreateTextIndex(ctx, languageCode, fieldNames...)
}

// FindOne finds one document in the collection using filter.
// It returns ErrNotFound if no document is found.
func FindOne[T any](ctx context.Context, coll *Collection, filter M, opts ...FindOptions) (T, error) {
	var result T
	if err := coll.FindOne(ctx, &result, filter, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// Find finds many documents in the collection using filter.
func Find[T any](ctx context.Context, coll *Collection, filter M, opts ...FindOptions) ([]T, error) {
	var result []T
	if err := coll.Find(ctx, &result, filter, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// FindAll finds all documents in the collection.
func FindAll[T any](ctx context.Context, coll *Collection, opts ...FindOptions) ([]T, error) {
	var result []T
	if err := coll.FindAll(ctx, &result, opts...); err != nil {
		return result, err
	}
	return result, nil
}

// Count counts the number of documents in the collection using filter.
func Count(ctx context.Context, coll *Collection, filter M) (int64, error) {
	return coll.Count(ctx, filter)
}

// Distinct finds distinct values for the specified field in the collection.
func Distinct[T any](ctx context.Context, coll *Collection, field string, filter M) ([]T, error) {
	var result []T
	if err := coll.Distinct(ctx, &result, field, filter); err != nil {
		return result, err
	}
	return result, nil
}

// Insert inserts a document(s) into the collection.
func Insert(ctx context.Context, coll *Collection, record ...any) error {
	return coll.Insert(ctx, record...)
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist.
// It returns ErrNotFound if no document is updated.
func Upsert(ctx context.Context, coll *Collection, record any, filter M) error {
	return coll.Upsert(ctx, record, filter)
}

// ReplaceOne replaces a document in the collection.
// It returns ErrNotFound if no document is updated.
func ReplaceOne(ctx context.Context, coll *Collection, record any, filter M) error {
	return coll.ReplaceOne(ctx, record, filter)
}

// SetFields sets fields in a document in the collection using updates map.
// It returns ErrNotFound if no document is updated.
func SetFields(ctx context.Context, coll *Collection, filter M, update map[string]any) error {
	return coll.SetFields(ctx, filter, update)
}

// UpdateOne updates a document in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// It returns ErrNotFound if no document is updated.
func UpdateOne(ctx context.Context, coll *Collection, filter, update M) error {
	return coll.UpdateOne(ctx, filter, update)
}

// UpdateMany updates multi documents in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// It returns ErrNotFound if no document is updated.
func UpdateMany(ctx context.Context, coll *Collection, filter, update M) error {
	return coll.UpdateMany(ctx, filter, update)
}

// UpdateOneFromDiff sets fields in a document in the collection using diff structure.
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
// It returns ErrNotFound if no document is deleted.
func DeleteMany(ctx context.Context, coll *Collection, filter M) error {
	return coll.DeleteMany(ctx, filter)
}
