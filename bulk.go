package mongox

import (
	"fmt"
	"sync"

	"github.com/maxbolgarin/lang"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// BulkBuilder is a builder for bulk operations.
// It is thread-safe. Empty builder is ready to use.
type BulkBuilder struct {
	models []mongo.WriteModel
	mu     sync.Mutex
}

// NewBulkBuilder returns a new instance of [BulkBuilder].
func NewBulkBuilder() *BulkBuilder {
	return &BulkBuilder{}
}

// Models returns the list of models added to the builder.
func (b *BulkBuilder) Models() []mongo.WriteModel {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.models
}

// Insert adds [mongo.InsertOneModel] to the [BulkBuilder] for every record in the slice.
func (b *BulkBuilder) Insert(records ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, r := range records {
		b.models = append(b.models, mongo.NewInsertOneModel().SetDocument(r))
	}
}

// Upsert adds [mongo.ReplaceOneModel] to the [BulkBuilder] for record with filter and upsert == true.
func (b *BulkBuilder) Upsert(record any, filter M) {
	m := mongo.NewReplaceOneModel().SetUpsert(true).SetFilter(filter.Prepare()).SetReplacement(record)
	b.addModel(m)
}

// Replace adds [mongo.ReplaceOneModel] to the [BulkBuilder] for record with filter.
func (b *BulkBuilder) ReplaceOne(record any, filter M) {
	m := mongo.NewReplaceOneModel().SetFilter(filter.Prepare()).SetReplacement(record)
	b.addModel(m)
}

// SetFields adds [mongo.UpdateOneModel] to the [BulkBuilder] for update with filter.
// For example: {key1: value1, key2: value2} becomes {$set: {key1: value1, key2: value2}}.
func (b *BulkBuilder) SetFields(filter, update M) {
	m := mongo.NewUpdateOneModel().SetFilter(filter.Prepare()).
		SetUpdate(lang.If(update != nil, prepareUpdates(update, Set), bson.D{}))
	b.addModel(m)
}

// UpdateOne adds [mongo.UpdateOneModel] to the [BulkBuilder] for update with filter.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
func (b *BulkBuilder) UpdateOne(filter, update M) {
	m := mongo.NewUpdateOneModel().SetFilter(filter.Prepare()).SetUpdate(update.Prepare())
	b.addModel(m)
}

// UpdateMany adds [mongo.UpdateManyModel] to the [BulkBuilder] for update with filter.
// Update map/document must contain key beginning with '$', e.g. {$set: {key1: value1}}.
// Modifiers operate on fields. For example: {$mod: {<field>: ...}}.
// You can use predefined options from mongox, e.g. mongox.M{mongox.Inc: mongox.M{"number": 1}}.
func (b *BulkBuilder) UpdateMany(filter, update M) {
	m := mongo.NewUpdateManyModel().SetFilter(filter.Prepare()).SetUpdate(update.Prepare())
	b.addModel(m)
}

// UpdateOneFromDiff adds [mongo.UpdateOneModel] to the [BulkBuilder] for diff with filter.
// Diff structure is a map of pointers to field names with their new values.
// E.g. if you have structure:
//
//	type MyStruct struct {name string, index int}
//
// Diff structure will be:
//
//	type MyStructDiff struct {name *string, index *int}
//
// It returns error if diff structure is invalid.
func (b *BulkBuilder) UpdateOneFromDiff(filter M, diff any) error {
	update, err := diffToUpdates(diff)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	m := mongo.NewUpdateOneModel().SetFilter(filter.Prepare()).SetUpdate(update)
	b.addModel(m)
	return nil
}

// DeleteFields adds [mongo.UpdateOneModel] to the [BulkBuilder] for update with filter and fields.
// For example: [key1, key2] becomes {$unset: {key1: "", key2: ""}}.
func (b *BulkBuilder) DeleteFields(filter M, fields ...string) {
	updateInfo := make(map[string]any, len(fields))
	for _, f := range fields {
		updateInfo[f] = ""
	}
	m := mongo.NewUpdateOneModel().SetFilter(filter.Prepare()).SetUpdate(prepareUpdates(updateInfo, Unset))
	b.addModel(m)
}

// DeleteOne adds [mongo.DeleteOneModel] to the [BulkBuilder] with filter.
func (b *BulkBuilder) DeleteOne(filter M) {
	m := mongo.NewDeleteOneModel().SetFilter(filter.Prepare())
	b.addModel(m)
}

// DeleteMany adds [mongo.DeleteManyModel] to the [BulkBuilder] with filter.
func (b *BulkBuilder) DeleteMany(filter M) {
	m := mongo.NewDeleteManyModel().SetFilter(filter.Prepare())
	b.addModel(m)
}

func (b *BulkBuilder) addModel(model mongo.WriteModel) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.models = append(b.models, model)
}
