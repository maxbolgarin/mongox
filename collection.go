package mongox

import (
	"context"
	"fmt"
	"strings"

	"github.com/maxbolgarin/lang"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection handles interactions with a MongoDB collection.
// It is safe for concurrent use by multiple goroutines.
type Collection struct {
	coll *mongo.Collection
}

// Collection returns the collection object.
func (m *Collection) Collection() *mongo.Collection {
	return m.coll
}

// CreateIndex creates an index for a collection with the given field names.
func (m *Collection) CreateIndex(ctx context.Context, isUnique bool, fieldNames ...string) error {
	indexModel := mongo.IndexModel{
		Options: options.Index().SetUnique(isUnique).SetName(
			m.coll.Name() + "_" + strings.Join(fieldNames, "_") + lang.If(isUnique, "_unique", "") + "_index"),
	}

	keys := make(bson.D, 0, len(fieldNames))
	for _, field := range fieldNames {
		keys = append(keys, bson.E{
			Key:   field,
			Value: 1,
		})
	}
	indexModel.Keys = keys

	if _, err := m.coll.Indexes().CreateOne(ctx, indexModel); err != nil {
		return err
	}

	return nil
}

// CreateTextIndex creates a text index for a collection with the given field names and language code.
// If the language code is not provided, "en" will be used by default.
func (m *Collection) CreateTextIndex(ctx context.Context, languageCode string, fieldNames ...string) error {
	if languageCode == "" {
		languageCode = "en"
	}
	if !supportedLanguages[languageCode] {
		return fmt.Errorf("unsupported language: %s", languageCode)
	}
	indexModel := mongo.IndexModel{
		Options: options.Index().SetDefaultLanguage(languageCode).SetName(
			m.coll.Name() + "_" + strings.Join(fieldNames, "_") + "_" + languageCode + "_text_index"),
	}

	keys := make(bson.D, 0, len(fieldNames))
	for _, field := range fieldNames {
		keys = append(keys, bson.E{
			Key:   field,
			Value: "text",
		})
	}
	indexModel.Keys = keys

	if _, err := m.coll.Indexes().CreateOne(ctx, indexModel); err != nil {
		return err
	}

	return nil
}

// FindOne finds one document in the collection using filter.
// It returns ErrNotFound if no document is found.
func (m *Collection) FindOne(ctx context.Context, dest any, filter M) error {
	res := m.coll.FindOne(ctx, filter.Prepare())
	if err := res.Err(); err != nil {
		return handleError(err)
	}
	if err := res.Decode(dest); err != nil {
		return handleError(err)
	}
	return nil
}

// Distinct finds distinct values for the specified field in the collection.
func (m *Collection) Distinct(ctx context.Context, dest any, field string, filter M) error {
	res := m.coll.Distinct(ctx, field, filter.Prepare())
	if err := res.Err(); err != nil {
		return handleError(err)
	}
	if err := res.Decode(dest); err != nil {
		return handleError(err)
	}
	return nil
}

// Find finds many documents in the collection using filter.
// It does not return any error if no document is found.
func (m *Collection) Find(ctx context.Context, dest any, filter M) error {
	return m.find(ctx, dest, filter.Prepare())
}

// FindAll finds all documents in the collection.
// It does not return any error if no document is found.
func (m *Collection) FindAll(ctx context.Context, dest any) error {
	return m.find(ctx, dest, bson.D{})
}

// Count counts the number of documents in the collection using filter.
func (m *Collection) Count(ctx context.Context, filter M) (int64, error) {
	count, err := m.coll.CountDocuments(ctx, filter.Prepare())
	if err != nil {
		return 0, handleError(err)
	}
	return count, nil
}

// Insert inserts a document(s) into the collection. Internally InsertMany uses bulk write.
func (m *Collection) Insert(ctx context.Context, records ...any) (err error) {
	if len(records) == 0 {
		return nil
	}
	if len(records) == 1 {
		_, err = m.coll.InsertOne(ctx, records[0])
	} else {
		_, err = m.coll.InsertMany(ctx, records)
	}
	return handleError(err)
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist.
func (m *Collection) Upsert(ctx context.Context, record any, filter M) error {
	opts := options.Replace().SetUpsert(true)
	_, err := m.coll.ReplaceOne(ctx, filter.Prepare(), record, opts)
	if err != nil {
		return handleError(err)
	}
	return nil
}

// SetFields sets fields in a document in the collection using updates map.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}
func (m *Collection) SetFields(ctx context.Context, filter M, update map[string]any) error {
	return m.updateOne(ctx, filter, prepareUpdates(update, Set))
}

// UpdateOne updates a document in the collection.
func (m *Collection) UpdateOne(ctx context.Context, filter, update M) error {
	return m.updateOne(ctx, filter, update.Prepare())
}

// UpdateMany updates multi documents in the collection.
func (m *Collection) UpdateMany(ctx context.Context, filter, update M) error {
	updateResult, err := m.coll.UpdateMany(ctx, filter.Prepare(), update)
	if err != nil {
		return handleError(err)
	}
	if updateResult != nil && updateResult.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateFromDiff sets fields in a document in the collection using diff structure.
func (m *Collection) UpdateFromDiff(ctx context.Context, filter M, diff any) error {
	update, err := diffToUpdates(diff)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return m.updateOne(ctx, filter, update)
}

// DeleteFields deletes fields in a document in the collection.
// For example: [key1, key2] becomes {$unset: {key1: "", key2: ""}}
func (m *Collection) DeleteFields(ctx context.Context, filter M, fields ...string) error {
	updateInfo := make(map[string]any, len(fields))
	for _, f := range fields {
		updateInfo[f] = ""
	}
	return m.updateOne(ctx, filter, prepareUpdates(updateInfo, Unset))
}

// DeleteOne deletes a document in the collection based on the filter.
func (m *Collection) DeleteOne(ctx context.Context, filter M) error {
	_, err := m.coll.DeleteOne(ctx, filter.Prepare())
	if err != nil {
		return handleError(err)
	}
	return nil
}

// DeleteMany deletes many documents in the collection based on the filter.
func (m *Collection) DeleteMany(ctx context.Context, filter M) error {
	_, err := m.coll.DeleteMany(ctx, filter.Prepare())
	if err != nil {
		return handleError(err)
	}
	return nil
}

func (m *Collection) find(ctx context.Context, dest any, filter bson.D) error {
	cur, err := m.coll.Find(ctx, filter)
	if err != nil {
		return handleError(err)
	}
	defer cur.Close(ctx)

	if err := cur.All(ctx, dest); err != nil {
		return handleError(err)
	}

	if err := cur.Err(); err != nil {
		return handleError(err)
	}

	return nil
}

func (m *Collection) updateOne(ctx context.Context, filter M, update bson.D) error {
	updateResult, err := m.coll.UpdateOne(ctx, filter.Prepare(), update)
	if err != nil {
		return handleError(err)
	}
	if updateResult != nil && updateResult.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}
