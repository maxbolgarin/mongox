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

// FindOptions is used to configure FindOne, Find and FindAll operations.
type FindOptions struct {
	// Sets a limit of documents returned in the result set.
	// No-op in FindOne.
	Limit int
	// How many documents to skip before returning the first document in the result set.
	Skip int
	// The order of the documents returned in the result set. Fields specified in the sort, must have an index.
	// Sort has priority over SortMany.
	// Example: mongox.M{"name": 1} - sort by name in ascending order.
	Sort M
	// The order of the documents returned in the result set. Fields specified in the sort, must have an index.
	// Sort has priority over SortMany.
	// Example: []mongox.M{{"name": mongox.Ascending}, {"age": mongox.Descending}} - sort by name in ascending order and then by age in descending order.
	SortMany []M
	// For queries against a sharded collection, allows the command to return partial results,
	// rather than an error, if one or more queried shards are unavailable.
	AllowPartialResults bool
	// Whether or not pipelines that require more than 100 megabytes of memory to execute write to temporary files on disk.
	// No-op in FindOne.
	AllowDiskUse bool
}

// Collection handles interactions with a MongoDB collection.
// It is safe for concurrent use by multiple goroutines.
type Collection struct {
	coll *mongo.Collection
}

// Name returns the name of the collection.
func (m *Collection) Name() string {
	return m.coll.Name()
}

// Collection returns an original mongo.Collection object.
func (m *Collection) Collection() *mongo.Collection {
	return m.coll
}

// CreateIndex creates an index for a collection with the given field names.
// Field names are required and must be unique.
func (m *Collection) CreateIndex(ctx context.Context, isUnique bool, fieldNames ...string) error {
	if len(fieldNames) == 0 {
		return fmt.Errorf("%w: no field names provided", ErrInvalidArgument)
	}

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
		return HandleMongoError(err)
	}

	return nil
}

// CreateTextIndex creates a text index for a collection with the given field names and language code.
// You should create a text index to use text search. Field names are required and must be unique.
// If the language code is not provided, "en" will be used by default.
func (m *Collection) CreateTextIndex(ctx context.Context, languageCode string, fieldNames ...string) error {
	if len(fieldNames) == 0 {
		return fmt.Errorf("%w: no field names provided", ErrInvalidArgument)
	}

	if languageCode == "" {
		languageCode = "en"
	}
	if !supportedLanguages[languageCode] {
		return fmt.Errorf("%w: %s", ErrUnsupportedLanguage, languageCode)
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
		return HandleMongoError(err)
	}

	return nil
}

// FindOne finds a one document in the collection using filter.
// It returns ErrNotFound if NO document is found.
// Limit and AllowDiskUse options are no-op.
func (m *Collection) FindOne(ctx context.Context, dest any, filter M, rawOpts ...FindOptions) error {
	res := m.coll.FindOne(ctx, filter.Prepare(), setFindOneOptions(rawOpts...))
	if err := res.Err(); err != nil {
		return HandleMongoError(err)
	}
	if err := res.Decode(dest); err != nil {
		return HandleMongoError(err)
	}
	return nil
}

// Find finds many documents in the collection using filter.
// It does NOT return any error if no document is found.
func (m *Collection) Find(ctx context.Context, dest any, filter M, opts ...FindOptions) error {
	return m.find(ctx, dest, filter.Prepare(), opts...)
}

// FindAll finds all documents in the collection.
// It does NOT return any error if no document is found.
func (m *Collection) FindAll(ctx context.Context, dest any, opts ...FindOptions) error {
	return m.find(ctx, dest, bson.D{}, opts...)
}

// FindOneAndDelete finds a document in the collection using filter and deletes it.
// It returns ErrNotFound if no document is found.
func (m *Collection) FindOneAndDelete(ctx context.Context, dest any, filter M) error {
	res := m.coll.FindOneAndDelete(ctx, filter.Prepare())
	if err := res.Err(); err != nil {
		return HandleMongoError(err)
	}
	if err := res.Decode(dest); err != nil {
		return HandleMongoError(err)
	}
	return nil
}

// FindOneAndReplace finds a document in the collection using filter and replaces it.
// It returns ErrNotFound if no document is found.
func (m *Collection) FindOneAndReplace(ctx context.Context, dest any, filter M, replacement any) error {
	res := m.coll.FindOneAndReplace(ctx, filter.Prepare(), replacement)
	if err := res.Err(); err != nil {
		return HandleMongoError(err)
	}
	if err := res.Decode(dest); err != nil {
		return HandleMongoError(err)
	}
	return nil
}

// FindOneAndUpdate finds a document in the collection using filter and updates it.
// It returns ErrNotFound if no document is found.
func (m *Collection) FindOneAndUpdate(ctx context.Context, dest any, filter M, update any) error {
	res := m.coll.FindOneAndUpdate(ctx, filter.Prepare(), update)
	if err := res.Err(); err != nil {
		return HandleMongoError(err)
	}
	if err := res.Decode(dest); err != nil {
		return HandleMongoError(err)
	}
	return nil
}

// Count counts the number of documents in the collection using filter.
// Nil filter means count all documents.
func (m *Collection) Count(ctx context.Context, filter M) (int64, error) {
	count, err := m.coll.CountDocuments(ctx, filter.Prepare())
	if err != nil {
		return 0, HandleMongoError(err)
	}
	return count, nil
}

// Distinct finds distinct values for the specified field in the collection using filter.
func (m *Collection) Distinct(ctx context.Context, dest any, field string, filter M) error {
	if field == "" {
		return fmt.Errorf("%w: no field name provided", ErrInvalidArgument)
	}
	res := m.coll.Distinct(ctx, field, filter.Prepare())
	if err := res.Err(); err != nil {
		return HandleMongoError(err)
	}
	if err := res.Decode(dest); err != nil {
		return HandleMongoError(err)
	}
	return nil
}

// InsertOne inserts a document into the collection.
// It returns ID of the inserted document.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// It returns ErrInternal if no inserted ID is returned.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (m *Collection) InsertOne(ctx context.Context, record any, isStrictID ...bool) (id bson.ObjectID, err error) {
	ids, err := m.InsertMany(ctx, []any{record}, isStrictID...)
	if err != nil {
		return bson.ObjectID{}, err
	}
	if len(ids) == 0 {
		return bson.ObjectID{}, fmt.Errorf("%w: no inserted ID", ErrInternal)
	}
	return ids[0], nil
}

// Insert inserts a document or many documents into the collection.
// It returns IDs of the inserted documents. Internally InsertMany uses bulk write.
// It NOT returns an error if inserted IDs are not ObjectID, so it is NOT strict.
// If inserted ID is not an ObjectID, it will be returned as empty bson.ObjectID.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (m *Collection) Insert(ctx context.Context, records ...any) (ids []bson.ObjectID, err error) {
	return m.InsertMany(ctx, records)
}

// InsertStrict inserts a document or many documents into the collection.
// It returns IDs of the inserted documents. Internally InsertMany uses bulk write.
// It returns an error if inserted IDs are not ObjectID.
func (m *Collection) InsertStrict(ctx context.Context, records ...any) (ids []bson.ObjectID, err error) {
	return m.InsertMany(ctx, records, true)
}

// InsertMany inserts many documents into the collection.
// It returns IDs of the inserted documents.
// Internally InsertMany uses bulk write.
// If isStrictID is true, it will return an error if the inserted ID is not an ObjectID.
// If isStrictID is false and if inserted ID is not an ObjectID, it will be returned as empty bson.ObjectID.
// If you provide your own ID, it is assumed you already know it, so it will not be returned.
func (m *Collection) InsertMany(ctx context.Context, records []any, isStrictID ...bool) (ids []bson.ObjectID, err error) {
	if len(records) == 0 {
		return nil, nil
	}

	ids = make([]bson.ObjectID, len(records))
	var ok bool
	if len(records) == 1 {
		res, err := m.coll.InsertOne(ctx, records[0])
		if err != nil {
			return nil, HandleMongoError(err)
		}
		ids[0], ok = res.InsertedID.(bson.ObjectID)
		if !ok && len(isStrictID) > 0 && isStrictID[0] {
			return nil, fmt.Errorf("%w: expected ObjectID, got %T, %v", ErrInvalidArgument, res.InsertedID, res.InsertedID)
		}

	} else {
		var errs []string
		res, err := m.coll.InsertMany(ctx, records)
		if err != nil {
			return nil, HandleMongoError(err)
		}
		for i, id := range res.InsertedIDs {
			ids[i], ok = id.(bson.ObjectID)
			if !ok && len(isStrictID) > 0 && isStrictID[0] {
				errs = append(errs, fmt.Sprintf("expected ObjectID, got %T, %v", id, id))
			}
		}
		if len(errs) > 0 {
			return nil, fmt.Errorf("%w: %v", ErrInvalidArgument, strings.Join(errs, ", "))
		}
	}
	return ids, nil
}

// Upsert replaces a document in the collection or inserts it if it doesn't exist.
// It returns ID of the interserted document.
// If existing document is updated (no new inserted), it returns nil ID and nil error.
// If no document is updated, it returns nil ID and ErrNotFound.
func (m *Collection) Upsert(ctx context.Context, record any, filter M) (*bson.ObjectID, error) {
	opts := options.Replace().SetUpsert(true)
	upd, err := m.coll.ReplaceOne(ctx, filter.Prepare(), record, opts)
	if err != nil {
		return nil, HandleMongoError(err)
	}
	if upd != nil {
		if upd.MatchedCount == 0 && upd.UpsertedCount == 0 {
			return nil, ErrNotFound
		}
		if upd.UpsertedID != nil {
			id := upd.UpsertedID.(bson.ObjectID)
			return &id, nil
		}
	}
	return nil, nil
}

// ReplaceOne replaces a document in the collection.
// It returns ErrNotFound if no document is updated.
func (m *Collection) ReplaceOne(ctx context.Context, record any, filter M) error {
	upd, err := m.coll.ReplaceOne(ctx, filter.Prepare(), record)
	if err != nil {
		return HandleMongoError(err)
	}
	if upd != nil && upd.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// SetFields sets fields in a document in the collection using updates map.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}.
// It returns ErrNotFound if no document is updated.
func (m *Collection) SetFields(ctx context.Context, filter, update M) error {
	return m.updateOne(ctx, filter.Prepare(), lang.If(update != nil, prepareUpdates(update, Set), bson.D{}))
}

// UpdateOne updates a document in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It returns ErrNotFound if no document is updated.
func (m *Collection) UpdateOne(ctx context.Context, filter, update M) error {
	return m.updateOne(ctx, filter.Prepare(), update.Prepare())
}

// UpdateMany updates multi documents in the collection.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
// It returns number of updated documents.
// It returns ErrNotFound if no document is updated.
func (m *Collection) UpdateMany(ctx context.Context, filter, update M) (int, error) {
	updateResult, err := m.coll.UpdateMany(ctx, filter.Prepare(), update.Prepare())
	if err != nil {
		return 0, HandleMongoError(err)
	}
	if updateResult != nil && updateResult.MatchedCount == 0 {
		return 0, ErrNotFound
	}
	return int(updateResult.ModifiedCount), nil
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
func (m *Collection) UpdateOneFromDiff(ctx context.Context, filter M, diff any) error {
	update, err := diffToUpdates(diff)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return m.updateOne(ctx, filter.Prepare(), update)
}

// DeleteFields deletes fields in a document in the collection.
// For example: [key1, key2] becomes {$unset: {key1: "", key2: ""}}.
// It returns ErrNotFound if no document is updated.
func (m *Collection) DeleteFields(ctx context.Context, filter M, fields ...string) error {
	updateInfo := make(map[string]any, len(fields))
	for _, f := range fields {
		updateInfo[f] = ""
	}
	return m.updateOne(ctx, filter.Prepare(), prepareUpdates(updateInfo, Unset))
}

// DeleteOne deletes a document in the collection based on the filter.
// It returns ErrNotFound if no document is deleted.
func (m *Collection) DeleteOne(ctx context.Context, filter M) error {
	del, err := m.coll.DeleteOne(ctx, filter.Prepare())
	if err != nil {
		return HandleMongoError(err)
	}
	if del != nil && del.DeletedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteMany deletes many documents in the collection based on the filter.
// It returns number of deleted documents.
// It returns ErrNotFound if no document is deleted.
func (m *Collection) DeleteMany(ctx context.Context, filter M) (int, error) {
	del, err := m.coll.DeleteMany(ctx, filter.Prepare())
	if err != nil {
		return 0, HandleMongoError(err)
	}
	if del != nil && del.DeletedCount == 0 {
		return 0, ErrNotFound
	}
	return int(del.DeletedCount), nil
}

// BulkWrite executes bulk write operations in the collection.
// Use [BulkBuilder] to create models for bulk write operations.
// IsOrdered==true means that all operations are executed in the order they are added to the [BulkBuilder]
// and if any of them fails, the whole operation fails. Error is not returning.
// IsOrdered==false means that all operations are executed in parallel and if any of them fails,
// the whole operation continues. Error is not returning.
// It returns ErrNotFound if no document is matched/inserted/updated/deleted.
func (m *Collection) BulkWrite(ctx context.Context, models []mongo.WriteModel, isOrdered bool) (mongo.BulkWriteResult, error) {
	opts := options.BulkWrite().SetOrdered(isOrdered)
	res, err := m.coll.BulkWrite(ctx, models, opts)
	if err != nil {
		return mongo.BulkWriteResult{}, HandleMongoError(err)
	}
	if res != nil && res.MatchedCount+res.DeletedCount+res.InsertedCount+res.ModifiedCount+res.UpsertedCount == 0 {
		return mongo.BulkWriteResult{}, ErrNotFound
	}
	return lang.Deref(res), nil
}

func (m *Collection) find(ctx context.Context, dest any, filter bson.D, rawOpts ...FindOptions) error {
	cur, err := m.coll.Find(ctx, filter, setFindOptions(rawOpts...))
	if err != nil {
		return HandleMongoError(err)
	}
	defer cur.Close(ctx)

	if err := cur.All(ctx, dest); err != nil {
		return HandleMongoError(err)
	}

	if err := cur.Err(); err != nil {
		return HandleMongoError(err)
	}

	return nil
}

func (m *Collection) updateOne(ctx context.Context, filter, update bson.D, opts ...options.Lister[options.UpdateOneOptions]) error {
	updateResult, err := m.coll.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return HandleMongoError(err)
	}
	if updateResult != nil && updateResult.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

func setFindOneOptions(rawOpts ...FindOptions) *options.FindOneOptionsBuilder {
	findOneOpts := options.FindOne()
	if len(rawOpts) > 0 {
		opts := rawOpts[0]
		lang.IfF(opts.Skip > 0, func() { findOneOpts.SetSkip(int64(opts.Skip)) })
		lang.IfF(opts.AllowPartialResults, func() { findOneOpts.SetAllowPartialResults(opts.AllowPartialResults) })

		lang.IfF(len(opts.SortMany) > 0, func() {
			sortMany := make(bson.D, 0, len(opts.SortMany))
			for _, sort := range opts.SortMany {
				for k, v := range sort {
					sortMany = append(sortMany, bson.E{Key: k, Value: v})
				}
			}
			findOneOpts.SetSort(sortMany)
		})
		lang.IfF(len(opts.Sort) > 0, func() { findOneOpts.SetSort(opts.Sort) }) // Sort has priority over SortMany
	}
	return findOneOpts
}

func setFindOptions(rawOpts ...FindOptions) *options.FindOptionsBuilder {
	findOpts := options.Find()
	if len(rawOpts) > 0 {
		opts := rawOpts[0]
		lang.IfF(opts.Limit > 0, func() { findOpts.SetLimit(int64(opts.Limit)) })
		lang.IfF(opts.Skip > 0, func() { findOpts.SetSkip(int64(opts.Skip)) })
		lang.IfF(opts.AllowPartialResults, func() { findOpts.SetAllowPartialResults(opts.AllowPartialResults) })
		lang.IfF(opts.AllowDiskUse, func() { findOpts.SetAllowDiskUse(opts.AllowDiskUse) })
		lang.IfF(len(opts.SortMany) > 0, func() {
			sortMany := make(bson.D, 0, len(opts.SortMany))
			for _, sort := range opts.SortMany {
				for k, v := range sort {
					sortMany = append(sortMany, bson.E{Key: k, Value: v})
				}
			}
			findOpts.SetSort(sortMany)
		})
		lang.IfF(len(opts.Sort) > 0, func() { findOpts.SetSort(opts.Sort) }) // Sort has priority over SortMany
	}
	return findOpts
}
