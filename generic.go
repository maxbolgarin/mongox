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
func FindOne[T any](ctx context.Context, coll *Collection, filter F) (T, error) {
	var result T
	if err := coll.FindOne(ctx, &result, filter); err != nil {
		return result, err
	}
	return result, nil
}

// Distinct finds distinct values for the specified field in the collection.
func Distinct[T any](ctx context.Context, coll *Collection, field string, filter F) ([]T, error) {
	var result []T
	if err := coll.Distinct(ctx, &result, field, filter); err != nil {
		return result, err
	}
	return result, nil
}

// FindMany finds many documents in the collection using filter.
func FindMany[T any](ctx context.Context, coll *Collection, filter F) ([]T, error) {
	var result []T
	if err := coll.FindMany(ctx, &result, filter); err != nil {
		return result, err
	}
	return result, nil
}

// FindAll finds all documents in the collection.
func FindAll[T any](ctx context.Context, coll *Collection) ([]T, error) {
	var result []T
	if err := coll.FindAll(ctx, &result); err != nil {
		return result, err
	}
	return result, nil
}

// Count counts the number of documents in the collection using filter.
func Count(ctx context.Context, coll *Collection, filter F) (int64, error) {
	return coll.Count(ctx, filter)
}

// Insert inserts a document(s) into the collection.
func Insert(ctx context.Context, coll *Collection, record ...any) error {
	return coll.Insert(ctx, record...)
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist.
func Upsert(ctx context.Context, coll *Collection, record any, filter F) error {
	return coll.Upsert(ctx, record, filter)
}

// SetFields sets fields in a document in the collection using updates map.
func SetFields(ctx context.Context, coll *Collection, filter F, update map[string]any) error {
	return coll.SetFields(ctx, filter, update)
}

// UpdateOne updates a document in the collection.
func UpdateOne(ctx context.Context, coll *Collection, filter, update F) error {
	return coll.UpdateOne(ctx, filter, update)
}

// UpdateMany updates multi documents in the collection.
func UpdateMany(ctx context.Context, coll *Collection, filter, update F) error {
	return coll.UpdateMany(ctx, filter, update)
}

// UpdateFromDiff sets fields in a document in the collection using diff structure.
func UpdateFromDiff(ctx context.Context, coll *Collection, filter F, diff any) error {
	return coll.UpdateFromDiff(ctx, filter, diff)
}

// DeleteFields deletes fields in a document in the collection.
func DeleteFields(ctx context.Context, coll *Collection, filter F, fields ...string) error {
	return coll.DeleteFields(ctx, filter, fields...)
}

// DeleteOne deletes a document in the collection based on the filter.
func DeleteOne(ctx context.Context, coll *Collection, filter F) error {
	return coll.DeleteOne(ctx, filter)
}

// DeleteMany deletes documents in the collection based on the filter.
func DeleteMany(ctx context.Context, coll *Collection, filter F) error {
	return coll.DeleteMany(ctx, filter)
}
