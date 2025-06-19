package mongox_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"math/rand"

	"github.com/maxbolgarin/lang"
	"github.com/maxbolgarin/mongox"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var client *mongox.Client

const (
	dbName = "mongox"

	indexSingleCollection = "index_single"
	indexManyCollection   = "index_many"
	textCollection        = "text"

	findOneCollection = "find_one"
	findCollection    = "find"
	findAllCollection = "find_all"
	updateCollection  = "update"
	bulkCollection    = "bulk"

	errorNilArgCollection        = "error_nil_arguments"
	errorInvalidArgCollection    = "error_invalid_arguments"
	errorInvalidFilterCollection = "error_invalid_filters"
	errorInvalidUpdCollection    = "error_invalid_upd"
	errorInvalidStateCollection  = "error_invalid_state"

	asyncCollection = "async"
)

func TestIndexAndText(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)

	t.Run("IndexSingle", func(t *testing.T) {
		err := db.Collection(indexSingleCollection).CreateIndex(ctx, true, "id")
		if err != nil {
			t.Error(err)
		}
		entity1 := newTestEntity("1")
		_, err = db.Collection(indexSingleCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		_, err = db.Collection(indexSingleCollection).Insert(ctx, entity1)
		if err == nil {
			t.Error("expected error, got nil")
		}
		// No error
		err = mongox.CreateIndex(ctx, db.Collection(indexSingleCollection), true, "id")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("IndexMany", func(t *testing.T) {
		err := db.Collection(indexManyCollection).CreateIndex(ctx, true, "id", "name", "number")
		if err != nil {
			t.Error(err)
		}
		entity1 := newTestEntity("1")
		_, err = db.Collection(indexManyCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		_, err = db.Collection(indexManyCollection).Insert(ctx, entity1)
		if err == nil {
			t.Error("expected error, got nil")
		}
		_, err = db.Collection(indexManyCollection).Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}
		_, err = db.Collection(indexManyCollection).InsertMany(ctx, []any{newTestEntity("1"), newTestEntity("1"), newTestEntity("1")})
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Text", func(t *testing.T) {
		entity1 := newTestEntity("1")
		entity1.Name = "Running tool: /usr/local/go/bin/go test -timeout 45s -run ^TestFind$ github.com/maxbolgarin/mongox"
		_, err := db.Collection(textCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}

		entity2 := newTestEntity("2")
		entity2.Name = "Pairs in tool must be in the form NewF(key1, value1, key2, value2, ...)"
		_, err = db.Collection(textCollection).Insert(ctx, entity2)
		if err != nil {
			t.Error(err)
		}

		_, err = mongox.Find[testEntity](ctx, db.Collection(textCollection), mongox.M{"$text": mongox.M{"$search": "tool"}})
		if !errors.Is(err, mongox.ErrIndexNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrIndexNotFound, err)
		}

		err = db.Collection(textCollection).CreateTextIndex(ctx, "dummy", "name")
		if !errors.Is(err, mongox.ErrUnsupportedLanguage) {
			t.Errorf("expected error %v, got %v", mongox.ErrUnsupportedLanguage, err)
		}

		err = db.Collection(textCollection).CreateTextIndex(ctx, "", "name")
		if err != nil {
			t.Error(err)
		}

		// No error
		err = mongox.CreateTextIndex(ctx, db.Collection(textCollection), "en", "name")
		if err != nil {
			t.Error(err)
		}

		res, err := mongox.Find[testEntity](ctx, db.Collection(textCollection), mongox.M{"$text": mongox.M{"$search": "tool"}})
		if err != nil {
			t.Error(err)
		}
		if len(res) != 2 {
			t.Errorf("expected 2, got %v", len(res))
		}

		res2, err := mongox.FindOne[testEntity](ctx, db.Collection(textCollection), mongox.M{"$text": mongox.M{"$search": "tool"}})
		if err != nil {
			t.Error(err)
		}
		if res2.ID == "" {
			t.Errorf("expected not empty, got %v", res2.ID)
		}
	})
}

func TestInsertFindDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Client().Ping(ctx, nil); err != nil {
		t.Error(err)
	}

	db := client.Database(dbName)
	if err := db.Database().Client().Ping(ctx, nil); err != nil {
		t.Error(err)
	}

	t.Run("FindOne_Replace_Upsert_DeleteOne", func(t *testing.T) {
		entity1 := newTestEntity("1")
		_, err := db.WithTransaction(ctx, func(ctx context.Context) (any, error) {
			_, err := db.Collection(findOneCollection).Insert(ctx, entity1)
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		// Transaction is available only for replica sets or Sharded Clusters, not for standalone servers.
		if !errors.Is(err, mongox.ErrIllegalOperation) {
			t.Errorf("expected error %v, got %v", mongox.ErrIllegalOperation, err)
		}

		_, err = db.Collection(findOneCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		entity2 := newTestEntity("2")
		_, err = mongox.Insert(ctx, db.Collection(findOneCollection), entity2)
		if err != nil {
			t.Error(err)
		}
		_, err = db.Collection(findOneCollection).Insert(ctx, newTestEntity("3"), newTestEntity("4"))
		if err != nil {
			t.Error(err)
		}

		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "1"))
		testFindOne(t, ctx, db, entity1, mongox.M{"name": entity1.Name})
		testFindOne(t, ctx, db, entity1, mongox.M{"number": entity1.Number})
		testFindOne(t, ctx, db, entity1, mongox.M{"bool": entity1.Bool})
		testFindOne(t, ctx, db, entity1, mongox.M{"slice": entity1.Slice})
		testFindOne(t, ctx, db, entity1, mongox.M{"array": entity1.Array})
		testFindOne(t, ctx, db, entity1, mongox.M{"map.1": entity1.Map["1"]})
		testFindOne(t, ctx, db, entity1, mongox.M{"time": entity1.Time})
		testFindOne(t, ctx, db, entity1, mongox.M{"struct": entity1.Struct})
		testFindOne(t, ctx, db, entity1, mongox.M{"struct.name": entity1.Struct.Name})
		testFindOne(t, ctx, db, entity1, mongox.M{"id": mongox.M{mongox.Eq: "1"}})
		testFindOne(t, ctx, db, entity1, mongox.M{"id": mongox.M{mongox.Lte: "1"}})
		testFindOne(t, ctx, db, entity1, nil)

		testFindOne(t, ctx, db, entity1, mongox.M{"id": "222"}, mongox.ErrNotFound)
		testFindOne(t, ctx, db, entity1, mongox.M{"i": "1"}, mongox.ErrNotFound)

		oldName := entity1.Name
		entity1.Name = "another-name"
		if err := db.Collection(findOneCollection).ReplaceOne(ctx, entity1, mongox.M{"id": "1"}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "1"))
		testFindOne(t, ctx, db, entity1, mongox.M{"name": oldName}, mongox.ErrNotFound)
		testFindOne(t, ctx, db, entity1, mongox.M{"name": entity1.Name})

		oldNumber := entity1.Number
		entity1.Number = entity1.Number + 1
		if err := mongox.ReplaceOne(ctx, db.Collection(findOneCollection), entity1, mongox.M{"name": entity1.Name}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "1"))
		testFindOne(t, ctx, db, entity1, mongox.M{"number": oldNumber}, mongox.ErrNotFound)
		testFindOne(t, ctx, db, entity1, mongox.M{"number": entity1.Number})

		testFindOne(t, ctx, db, entity2, mongox.NewM("id", "2"))
		if _, err := mongox.Upsert(ctx, db.Collection(findOneCollection), entity1, mongox.M{"id": "2"}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "2"), mongox.ErrNotFound)

		if err := db.Collection(findOneCollection).DeleteOne(ctx, mongox.M{"id": "1"}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.M{"id": "1"})

		if err := mongox.DeleteOne(ctx, db.Collection(findOneCollection), mongox.M{"id": "1"}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.M{"id": "1"}, mongox.ErrNotFound)

		err = db.Collection(findOneCollection).DeleteOne(ctx, mongox.M{"id": "1"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrNotFound, err)
		}
	})

	t.Run("Find_DeleteMany", func(t *testing.T) {
		entity2, entity3, entity4 := newTestEntity("2"), newTestEntity("3"), newTestEntity("4")
		_, err := db.Collection(findCollection).Insert(ctx, entity2, entity3, entity4)
		if err != nil {
			t.Error(err)
		}
		testFind(t, ctx, db, []testEntity{entity2, entity3, entity4}, nil)
		testFind(t, ctx, db, []testEntity{entity2}, mongox.M{"id": "2"})
		testFind(t, ctx, db, []testEntity{entity3}, mongox.M{"name": entity3.Name})
		testFind(t, ctx, db, []testEntity{entity2, entity3}, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}})
		testFind(t, ctx, db, []testEntity{entity4}, mongox.M{"id": mongox.M{mongox.Nin: []string{"2", "3"}}})
		testFind(t, ctx, db, []testEntity{entity3, entity4}, mongox.M{"id": mongox.M{mongox.Ne: "2"}})
		testFind(t, ctx, db, []testEntity{entity3, entity4}, mongox.M{"id": mongox.M{mongox.Gt: "2"}})
		testFind(t, ctx, db, []testEntity{entity3}, mongox.NewM("id", mongox.NewM(mongox.Gt, "2", mongox.Lte, "4")).Add("name", entity3.Name))
		testFind(t, ctx, db, []testEntity{entity4}, mongox.M{"id": mongox.M{mongox.Gt: "2", mongox.Lte: "4"}, "name": mongox.M{mongox.Ne: entity3.Name}})
		testFind(t, ctx, db, []testEntity{entity3, entity4}, mongox.M{mongox.Or: []mongox.M{{"id": "3"}, {"name": entity4.Name}}})

		testFind(t, ctx, db, []testEntity{entity3, entity4}, mongox.M{mongox.Or: mongox.M{"id": "3", "name": entity4.Name}}, mongox.ErrBadValue)

		n, err := db.Collection(findCollection).DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}})
		if err != nil {
			t.Error(err)
		}
		if n != 2 {
			t.Errorf("expected %d, got %d", 2, n)
		}
		testFind(t, ctx, db, []testEntity{}, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}})

		n, err = mongox.DeleteMany(ctx, db.Collection(findCollection), nil)
		if err != nil {
			t.Error(err)
		}
		if n == 0 {
			t.Errorf("expected not zero, got %d", n)
		}
		testFind(t, ctx, db, []testEntity{}, nil)

		_, err = mongox.DeleteMany(ctx, db.Collection(findCollection), nil)
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrNotFound, err)
		}
	})

	t.Run("FindAll_Count_Distinct", func(t *testing.T) {
		var result []testEntity
		err := db.Collection(findAllCollection).FindAll(ctx, &result)
		if err != nil {
			t.Error(err)
		}

		for i := 0; i < 10; i++ {
			_, err = db.Collection(findAllCollection).Insert(ctx, newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)),
				newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)),
				newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)))
			if err != nil {
				t.Error(err)
			}
		}

		result = nil
		err = db.Collection(findAllCollection).FindAll(ctx, &result)
		if err != nil {
			t.Error(err)
		}
		if len(result) != 100 {
			t.Errorf("expected 100, got %d", len(result))
		}

		result, err = mongox.FindAll[testEntity](ctx, db.Collection(findAllCollection))
		if err != nil {
			t.Error(err)
		}
		if len(result) != 100 {
			t.Errorf("expected 100, got %d", len(result))
		}

		result, err = mongox.FindAll[testEntity](ctx, db.Collection(findAllCollection), mongox.FindOptions{Limit: 10})
		if err != nil {
			t.Error(err)
		}
		if len(result) != 10 {
			t.Errorf("expected 10, got %d", len(result))
		}

		result, err = mongox.FindAll[testEntity](ctx, db.Collection(findAllCollection), mongox.FindOptions{Skip: 90})
		if err != nil {
			t.Error(err)
		}
		if len(result) != 10 {
			t.Errorf("expected 10, got %d", len(result))
		}

		result, err = mongox.FindAll[testEntity](ctx, db.Collection(findAllCollection), mongox.FindOptions{SortMany: []mongox.M{{"id": mongox.Ascending}}})
		if err != nil {
			t.Error(err)
		}
		if len(result) != 100 {
			t.Errorf("expected 100, got %d", len(result))
		}

		res, err := db.Collection(findAllCollection).Count(ctx, nil)
		if err != nil {
			t.Error(err)
		}
		if res != 100 {
			t.Errorf("expected 100, got %d", res)
		}

		res, err = mongox.Count(ctx, db.Collection(findAllCollection), nil)
		if err != nil {
			t.Error(err)
		}
		if res != 100 {
			t.Errorf("expected 100, got %d", res)
		}

		res, err = db.Collection(findAllCollection).Count(ctx, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if res != 10 {
			t.Errorf("expected 10, got %d", res)
		}

		var ids []string
		err = db.Collection(findAllCollection).Distinct(ctx, &ids, "id", nil)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 10 {
			t.Errorf("expected 10, got %d", len(ids))
		}

		ids, err = mongox.Distinct[string](ctx, db.Collection(findAllCollection), "id", nil)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 10 {
			t.Errorf("expected 10, got %d", len(ids))
		}
	})
}

func TestUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)

	t.Run("Update", func(t *testing.T) {
		var (
			coll   = db.Collection(updateCollection)
			entity = newTestEntity("1")
			f      = mongox.M{"id": "1"}
		)

		_, err := coll.Insert(ctx, entity)
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, entity, mongox.M{"name": entity.Name})
		err = mongox.SetFields(ctx, coll, f, mongox.M{"name": "new-name"})
		if err != nil {
			t.Error(err)
		}
		testUpdate(t, ctx, db, entity, mongox.M{"name": entity.Name}, mongox.ErrNotFound)

		if err := coll.FindOne(ctx, &entity, f); err != nil {
			t.Error(err)
		}
		testUpdate(t, ctx, db, entity, mongox.M{"name": "new-name"})

		testUpdate(t, ctx, db, entity, mongox.M{"number": entity.Number})
		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Inc: mongox.M{"number": 10}})
		if err != nil {
			t.Error(err)
		}
		testUpdate(t, ctx, db, entity, mongox.M{"number": entity.Number}, mongox.ErrNotFound)

		if err := coll.FindOne(ctx, &entity, f); err != nil {
			t.Error(err)
		}
		testUpdate(t, ctx, db, entity, mongox.M{"number": entity.Number})

		err = coll.DeleteFields(ctx, f, "name")
		if err != nil {
			t.Error(err)
		}
		testUpdate(t, ctx, db, entity, mongox.M{"name": entity.Name}, mongox.ErrNotFound)
		testUpdate(t, ctx, db, entity, mongox.M{"name": "new-name"}, mongox.ErrNotFound)

		entity.Name = ""
		testUpdate(t, ctx, db, entity, mongox.M{"name": nil})

		_, err = coll.Insert(ctx, entity, entity, entity, entity, entity, entity, entity, entity, entity)
		if err != nil {
			t.Error(err)
		}

		var results []testEntity

		err = coll.Find(ctx, &results, f)
		if err != nil {
			t.Error(err)
		}
		if len(results) != 10 {
			t.Errorf("expected 10, got %d", len(results))
		}

		n, err := coll.UpdateMany(ctx, f, mongox.M{mongox.Set: mongox.M{"id": "2"}})
		if err != nil {
			t.Error(err)
		}
		if n != 10 {
			t.Errorf("expected 10, got %d", n)
		}

		results = nil
		err = coll.Find(ctx, &results, f)
		if err != nil {
			t.Error(err)
		}
		if len(results) != 0 {
			t.Errorf("expected 0, got %d", len(results))
		}

		err = coll.Find(ctx, &results, mongox.M{"id": "2"})
		if err != nil {
			t.Error(err)
		}
		if len(results) != 10 {
			t.Errorf("expected 10, got %d", len(results))
		}

		newEntity := newTestEntity("1")
		newEntity.InlineStruct = inlineStruct{
			InlineField: "inline-field",
		}
		_, err = coll.Insert(ctx, newEntity)
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity, mongox.M{"name": newEntity.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"number": newEntity.Number})
		testUpdate(t, ctx, db, newEntity, mongox.M{"struct.name": newEntity.Struct.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"time": newEntity.Time})
		testUpdate(t, ctx, db, newEntity, mongox.M{"inline_field": newEntity.InlineStruct.InlineField})

		updTestEntity := struct {
			Name   *string    `bson:"name"`
			Number *int       `bson:"number"`
			Time   *time.Time `bson:"time"`
			Struct *struct {
				Name *string `bson:"name"`
			} `bson:"struct"`
			InlineStruct *struct {
				InlineField *string `bson:"inline_field"`
			} `bson:"inline_struct,inline"`
		}{
			Name:   lang.Ptr("new-name"),
			Number: lang.Ptr(9999999),
			Time:   lang.Ptr(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			Struct: &struct {
				Name *string `bson:"name"`
			}{
				Name: lang.Ptr("new-struct-name"),
			},
			InlineStruct: &struct {
				InlineField *string `bson:"inline_field"`
			}{
				InlineField: lang.Ptr("new-inline-field"),
			},
		}

		err = mongox.UpdateOneFromDiff(ctx, coll, mongox.M{"id": "1"}, &updTestEntity)
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity, mongox.M{"name": newEntity.Name}, mongox.ErrNotFound)
		testUpdate(t, ctx, db, newEntity, mongox.M{"number": newEntity.Number}, mongox.ErrNotFound)
		testUpdate(t, ctx, db, newEntity, mongox.M{"struct.name": newEntity.Struct.Name}, mongox.ErrNotFound)
		testUpdate(t, ctx, db, newEntity, mongox.M{"time": newEntity.Time}, mongox.ErrNotFound)
		testUpdate(t, ctx, db, newEntity, mongox.M{"inline_field": newEntity.InlineStruct.InlineField}, mongox.ErrNotFound)

		newEntity, err = mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity, mongox.M{"name": newEntity.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"number": newEntity.Number})
		testUpdate(t, ctx, db, newEntity, mongox.M{"struct.name": newEntity.Struct.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"time": newEntity.Time})
		testUpdate(t, ctx, db, newEntity, mongox.M{"inline_field": newEntity.InlineStruct.InlineField})

		updTestEntity2 := struct {
			Struct *struct {
				Name *string
			}
		}{
			Struct: &struct {
				Name *string
			}{
				Name: lang.Ptr("new-struct-name-2"),
			},
		}

		err = mongox.UpdateOneFromDiff(ctx, coll, mongox.M{"id": "1"}, &updTestEntity2)
		if err != nil {
			t.Error(err)
		}

		newEntity2, err := mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity2, mongox.M{"struct.name": newEntity.Struct.Name})
		testUpdate(t, ctx, db, newEntity2, mongox.M{"Struct.Name": "new-struct-name-2"})

		updTestEntity3 := struct {
			Name *testUpdateEntity `bson:"name"`
		}{
			Name: &testUpdateEntity{name: "new-name-3"},
		}

		err = mongox.UpdateOneFromDiff(ctx, coll, mongox.M{"id": "1"}, &updTestEntity3)
		if err != nil {
			t.Error(err)
		}

		newEntity3, err := mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity3, mongox.M{"name": "new-name-3"})
	})
}

func TestBulk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)

	t.Run("BulkSuccess", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			var isOrdered bool
			coll := db.Collection(bulkCollection)
			if i == 1 {
				coll = db.Collection(bulkCollection + "1")
				isOrdered = true
			}

			bulker := mongox.NewBulkBuilder()
			for i := 0; i < 10; i++ {
				bulker.Insert(newTestEntity(fmt.Sprintf("%d", i+1)))
			}
			bulker.Upsert(newTestEntity("100"), mongox.M{"id": "11"})
			bulker.ReplaceOne(newTestEntity("200"), mongox.M{"id": "2"})
			bulker.SetFields(mongox.M{"id": "1"}, mongox.M{"name": "new-name"})
			bulker.UpdateOne(mongox.M{"id": "1"}, mongox.M{"$set": mongox.M{"number": 228}})
			bulker.UpdateMany(mongox.M{"id": mongox.M{mongox.Gte: "10"}}, mongox.M{"$set": mongox.M{"number": 322}})
			bulker.UpdateOneFromDiff(mongox.M{"id": "10"}, struct {
				Name *string `bson:"name"`
			}{
				Name: lang.Ptr("new-name-2"),
			})
			bulker.DeleteFields(mongox.M{"id": "10"}, "struct.name")
			bulker.DeleteOne(mongox.M{"id": "3"})
			bulker.DeleteMany(mongox.M{"id": mongox.M{mongox.In: []any{"4", "5"}}})

			_, err := mongox.BulkWrite(ctx, coll, bulker.Models(), isOrdered)
			if err != nil {
				t.Error(err)
			}

			n, err := coll.Count(ctx, nil)
			if err != nil {
				t.Error(err)
			}
			if n != 8 {
				t.Errorf("expected %d, got %d", 8, n)
			}

			entity, err := mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
			if err != nil {
				t.Error(err)
			}
			if entity.Name != "new-name" {
				t.Errorf("expected %s, got %s", "new-name", entity.Name)
			}
			if entity.Number != 228 {
				t.Errorf("expected %d, got %d", 228, entity.Number)
			}
			entity10, err := mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "10"})
			if err != nil {
				t.Error(err)
			}
			if entity10.Name != "new-name-2" {
				t.Errorf("expected %s, got %s", "new-name-2", entity10.Name)
			}
			if entity10.Number != 322 {
				t.Errorf("expected %d, got %d", 322, entity10.Number)
			}
			if entity10.Struct.Name != "" {
				t.Errorf("expected %s, got %s", "", entity10.Struct.Name)
			}

			testBulk(t, ctx, db, newTestEntity("1"), mongox.M{"id": "2"}, mongox.ErrNotFound)
			testBulk(t, ctx, db, newTestEntity("1"), mongox.M{"id": "3"}, mongox.ErrNotFound)
			testBulk(t, ctx, db, newTestEntity("1"), mongox.M{"id": "4"}, mongox.ErrNotFound)
			testBulk(t, ctx, db, newTestEntity("1"), mongox.M{"id": "5"}, mongox.ErrNotFound)
		}
	})

	t.Run("BulkError", func(t *testing.T) {
		coll := db.Collection(bulkCollection)
		bulker := mongox.NewBulkBuilder()
		bulker.DeleteOne(mongox.M{"id": "1"})

		_, err := mongox.BulkWrite(ctx, coll, bulker.Models(), false)
		if err != nil {
			t.Error(err)
		}

		_, err = mongox.BulkWrite(ctx, coll, bulker.Models(), true)
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrNotFound, err)
		}

		bulker.Insert(mongox.M{"id": "1"})

		_, err = mongox.BulkWrite(ctx, coll, bulker.Models(), false)
		if err != nil {
			t.Error(err)
		}

		_, err = mongox.BulkWrite(ctx, coll, bulker.Models(), true)
		if err != nil {
			t.Error(err)
		}

		bulker = mongox.NewBulkBuilder()
		bulker.Insert(mongox.M{"id": "1"})
		bulker.DeleteOne(mongox.M{"id": "2"})

		_, err = mongox.BulkWrite(ctx, coll, bulker.Models(), false)
		if err != nil {
			t.Error(err)
		}

		_, err = mongox.BulkWrite(ctx, coll, bulker.Models(), true)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)

	t.Run("Error_NilArguments", func(t *testing.T) {
		coll := db.Collection(errorNilArgCollection)
		if coll.Name() != errorNilArgCollection {
			t.Errorf("expected %v, got %v", errorNilArgCollection, coll.Name())
		}

		_, err := coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}

		err = coll.CreateIndex(ctx, true)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.CreateTextIndex(ctx, "")
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.FindOne(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.Find(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.FindAll(ctx, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		// No error
		_, err = coll.Count(ctx, nil)
		if err != nil {
			t.Error(err)
		}

		err = coll.Distinct(ctx, nil, "", nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		// No error
		err = coll.Distinct(ctx, nil, "id", nil)
		if err != nil {
			t.Error(err)
		}

		_, err = coll.Insert(ctx, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.Insert(ctx, []any{newTestEntity("2"), nil})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.Upsert(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.ReplaceOne(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.SetFields(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.UpdateOne(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.UpdateMany(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.UpdateOneFromDiff(ctx, nil, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.DeleteFields(ctx, nil)
		if err != nil {
			t.Error(err)
		}

		// No error
		err = coll.DeleteOne(ctx, nil)
		if err != nil {
			t.Error(err)
		}

		err = coll.DeleteOne(ctx, nil)
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrNotFound, err)
		}

		_, err = coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}

		// No error
		_, err = coll.DeleteMany(ctx, nil)
		if err != nil {
			t.Error(err)
		}

		_, err = coll.DeleteMany(ctx, nil)
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected error %v, got %v", mongox.ErrNotFound, err)
		}

		_, err = coll.BulkWrite(ctx, nil, false)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.BulkWrite(ctx, []mongo.WriteModel{mongo.NewDeleteManyModel(), nil}, false)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}
	})

	t.Run("Error_InvalidArguments", func(t *testing.T) {
		coll := db.Collection(errorInvalidArgCollection)

		_, err := coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}

		// Unexpected field, but it is ok for Mongo
		err = coll.CreateIndex(ctx, true, "dafasdas()++*88*;;ыупаыва")
		if err != nil {
			t.Error(err)
		}

		err = coll.CreateTextIndex(ctx, "fasdas", "dafasdas()++*88*;;ыупаыва")
		if !errors.Is(err, mongox.ErrUnsupportedLanguage) {
			t.Errorf("expected error %v, got %v", mongox.ErrUnsupportedLanguage, err)
		}

		// Unexpected field, but it is ok for Mongo
		err = coll.CreateTextIndex(ctx, "en", "dafasdas()++*88*;;ыупаыва")
		if err != nil {
			t.Error(err)
		}

		err = coll.FindOne(ctx, 1, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.Find(ctx, 1, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.FindAll(ctx, 1)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.Distinct(ctx, nil, "", nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		// No error
		err = coll.Distinct(ctx, 1, "id", nil)
		if err != nil {
			t.Error(err)
		}

		_, err = coll.Insert(ctx, 1)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.Insert(ctx, []any{newTestEntity("2"), 1})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.Upsert(ctx, 1, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.ReplaceOne(ctx, 1, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		var result testEntity
		err = coll.FindOne(ctx, result, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		var results []testEntity
		err = coll.Find(ctx, results, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.FindAll(ctx, results)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		var distinct2 []testEntity
		err = coll.Distinct(ctx, &distinct2, "id", nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.Find(ctx, &results, nil, mongox.FindOptions{
			Limit: -100,
		})
		if err != nil {
			t.Error(err)
		}

		err = coll.Find(ctx, &results, nil, mongox.FindOptions{
			Skip: -100,
		})
		if err != nil {
			t.Error(err)
		}

		err = coll.Find(ctx, &results, nil, mongox.FindOptions{
			Sort:                mongox.M{"fasdfdsafse": "asdasdea"},
			AllowPartialResults: true,
			AllowDiskUse:        true,
		})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.Find(ctx, &results, nil, mongox.FindOptions{
			Sort: mongox.M{"fasdfdsafse": mongox.M{"aaa": "bbb"}},
		})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			Limit:               -100,
			Skip:                -100,
			Sort:                mongox.M{"fasdfdsafse": "asdasdea"},
			AllowPartialResults: true,
			AllowDiskUse:        true,
		})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.BulkWrite(ctx, []mongo.WriteModel{mongo.NewDeleteManyModel()}, false)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}
	})

	t.Run("Error_InvalidFilter", func(t *testing.T) {
		coll := db.Collection(errorInvalidFilterCollection)

		_, err := coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}

		var (
			result  testEntity
			results []testEntity
			entity  = newTestEntity("1")
			upd     = mongox.M{mongox.Set: mongox.M{"id": "2"}}

			f = mongox.M{"aesdfsdf": lang.Ptr([]any{map[string]testEntity{"1": entity}, "()()()()fs`dvsrfvпцфкуапму<>>>>>>]]"})}
		)

		if err = coll.FindOne(ctx, &result, f); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if err = coll.Find(ctx, &results, f); err != nil {
			t.Error(err)
		}
		if _, err = coll.Count(ctx, f); err != nil {
			t.Error(err)
		}
		if err = coll.Distinct(ctx, nil, "f", f); err != nil {
			t.Error(err)
		}
		if err = coll.ReplaceOne(ctx, entity, f); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if err = coll.SetFields(ctx, f, upd); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if err = coll.UpdateOne(ctx, f, upd); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if _, err = coll.UpdateMany(ctx, f, upd); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if err = mongox.DeleteFields(ctx, coll, f); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if err = mongox.DeleteOne(ctx, coll, f); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if _, err = mongox.DeleteMany(ctx, coll, f); !errors.Is(err, mongox.ErrNotFound) {
			t.Error(err)
		}
		if _, err = coll.Upsert(ctx, entity, f); err != nil {
			t.Error("expected no error, got", err)
		}
	})

	t.Run("Error_InvalidUpdate", func(t *testing.T) {
		coll := db.Collection(errorInvalidUpdCollection)

		_, err := coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}

		var (
			f = mongox.M{"id": "1"}
		)

		err = coll.SetFields(ctx, f, mongox.NewM("()()()()fs`dvsrfvпцфкуапму<>>>>>>]]", lang.Ptr([]any{map[string]testEntity{"1": newTestEntity("2")}})))
		if err != nil {
			t.Error(err)
		}

		err = coll.SetFields(ctx, f, mongox.M{"number": mongox.CurrentDate})
		if err != nil {
			t.Error(err)
		}

		//

		err = coll.UpdateOne(ctx, f, mongox.M{"a": "b"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{"id": mongox.CurrentDate})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Inc: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Min: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Mul: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Rename: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Pop: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Push: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.AddToSet: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		//

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Inc: mongox.M{"id": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Inc: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Inc: mongox.M{"number": "1"}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		// No error
		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Min: mongox.M{"number": ""}})
		if err != nil {
			t.Error(err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Mul: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Rename: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = mongox.UpdateOne(ctx, coll, f, mongox.M{mongox.Pop: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.Push: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrBadValue) {
			t.Errorf("expected error %v, got %v", mongox.ErrBadValue, err)
		}

		err = coll.UpdateOne(ctx, f, mongox.M{mongox.AddToSet: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrBadValue) {
			t.Errorf("expected error %v, got %v", mongox.ErrBadValue, err)
		}

		// No error
		err = coll.UpdateOne(ctx, f, mongox.M{mongox.AddToSet: mongox.M{"slice": newTestEntity("1")}})
		if err != nil {
			t.Error(err)
		}

		//

		_, err = coll.UpdateMany(ctx, f, mongox.M{"a": "b"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{"a": "b"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{"id": mongox.CurrentDate})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Inc: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Min: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Mul: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Rename: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Pop: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Push: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.AddToSet: "id"})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		//

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Inc: mongox.M{"id": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Inc: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Inc: mongox.M{"number": "1"}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		// No error
		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Min: mongox.M{"number": ""}})
		if err != nil {
			t.Error(err)
		}

		_, err = mongox.UpdateMany(ctx, coll, f, mongox.M{mongox.Mul: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrTypeMismatch) {
			t.Errorf("expected error %v, got %v", mongox.ErrTypeMismatch, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Rename: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Pop: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrFailedToParse) {
			t.Errorf("expected error %v, got %v", mongox.ErrFailedToParse, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.Push: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrBadValue) {
			t.Errorf("expected error %v, got %v", mongox.ErrBadValue, err)
		}

		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.AddToSet: mongox.M{"number": ""}})
		if !errors.Is(err, mongox.ErrBadValue) {
			t.Errorf("expected error %v, got %v", mongox.ErrBadValue, err)
		}

		// No error
		_, err = coll.UpdateMany(ctx, f, mongox.M{mongox.AddToSet: mongox.M{"slice": newTestEntity("1")}})
		if err != nil {
			t.Error(err)
		}

		//

		err = coll.UpdateOneFromDiff(ctx, f, mongox.M{"a": "b"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}

		err = coll.UpdateOneFromDiff(ctx, f, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Error_InvalidState", func(t *testing.T) {
		coll := db.Collection(errorInvalidStateCollection)

		err := coll.CreateIndex(ctx, true, "id", "id", "id")
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}
		err = coll.CreateIndex(ctx, false, "id", "id", "id")
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}
		err = coll.CreateIndex(ctx, true, "id")
		if err != nil {
			t.Error(err)
		}
		err = coll.CreateIndex(ctx, false, "id")
		if err != nil {
			t.Error(err)
		}
		err = coll.CreateIndex(ctx, true, "id")
		if err != nil {
			t.Error(err)
		}
		err = coll.CreateIndex(ctx, false, "id")
		if err != nil {
			t.Error(err)
		}
		err = coll.CreateTextIndex(ctx, "en", "id", "id", "id")
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
		}
		err = coll.CreateTextIndex(ctx, "en", "id")
		if err != nil {
			t.Error(err)
		}
		err = coll.CreateTextIndex(ctx, "en", "id")
		if err != nil {
			t.Error(err)
		}
		_, err = coll.Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}
		_, err = coll.Insert(ctx, newTestEntity("1"))
		if !errors.Is(err, mongox.ErrDuplicate) {
			t.Errorf("expected error %v, got %v", mongox.ErrDuplicateKey, err)
		}
	})
}

func TestAsync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Error(err)
	}

	db := client.Database(dbName)
	asyncDB := client.AsyncDatabase(ctx, dbName, 0, slog.Default())

	t.Run("Async", func(t *testing.T) {
		coll := db.Collection(asyncCollection)
		if coll.Name() != asyncCollection {
			t.Errorf("expected %v, got %v", asyncCollection, coll.Name())
		}
		asyncColl := asyncDB.AsyncCollection(asyncCollection)
		queueColl := asyncColl.QueueCollection(asyncCollection)
		if asyncColl.Name() != asyncCollection {
			t.Errorf("expected %v, got %v", asyncCollection, asyncColl.Name())
		}
		if queueColl.Name() != asyncCollection {
			t.Errorf("expected %v, got %v", asyncCollection, queueColl.Name())
		}
		entity := newTestEntity("1")
		entity2 := newTestEntity("2")
		entity3 := newTestEntity("3")

		// error
		asyncDB.WithTransaction("", "", func(ctx context.Context) error {
			return nil
		})

		// error
		queueColl.Insert([]any{entity2, nil})

		for i := 0; i < 3; i++ {
			queueColl.Insert(entity)
		}

		bulk := mongox.NewBulkBuilder()
		for i := 0; i < 3; i++ {
			bulk.Insert(entity)
		}
		queueColl.BulkWrite(bulk.Models(), false)

		queueColl.Upsert(entity2, mongox.M{"id": "2"})
		queueColl.ReplaceOne(entity3, mongox.M{"id": "2"})
		queueColl.SetFields(mongox.M{"id": "3"}, mongox.M{"name": "new-name"})
		queueColl.UpdateOne(mongox.M{"id": "3"}, mongox.M{mongox.Inc: mongox.M{"number": 1}})
		diff := struct {
			Bool *bool `bson:"bool"`
		}{
			Bool: lang.Ptr(!entity3.Bool),
		}
		queueColl.UpdateOneFromDiff(mongox.M{"id": "3"}, diff)

		queueColl.UpdateMany(mongox.M{"id": "1"}, mongox.M{mongox.Inc: mongox.M{"number": 1}})
		queueColl.DeleteFields(mongox.M{"id": "3"}, "struct.name")
		queueColl.DeleteOne(mongox.M{"number": entity.Number + 1})
		queueColl.InsertMany([]any{entity, entity, entity})
		queueColl.DeleteMany(mongox.M{"number": entity.Number})

		var (
			s  []int
			mu sync.Mutex
			wg sync.WaitGroup
		)

		wg.Add(3)
		asyncDB.WithTask(asyncCollection, "", func(ctx context.Context) error {
			defer wg.Done()
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			s = append(s, 1)
			return nil
		})
		asyncDB.WithTask(asyncCollection, "", func(ctx context.Context) error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			s = append(s, 2)
			return nil
		})
		asyncDB.WithTask(asyncCollection, "", func(ctx context.Context) error {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			s = append(s, 3)
			return nil
		})

		wg.Wait()

		if !reflect.DeepEqual(s, []int{1, 2, 3}) {
			t.Errorf("expected %v, got %v", []int{1, 2, 3}, s)
		}

		n, err := coll.Count(ctx, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if n != 5 {
			t.Errorf("expected %v, got %v", 5, n)
		}

		entity.Number++
		testAsync(t, ctx, db, entity, mongox.M{"id": "1"})
		testAsync(t, ctx, db, entity2, mongox.M{"id": "2"}, mongox.ErrNotFound)

		entity3.Name = "new-name"
		entity3.Number++
		entity3.Bool = !entity3.Bool
		entity3.Struct.Name = ""
		testAsync(t, ctx, db, entity3, mongox.M{"id": "3"})
	})
}

func TestMain(m *testing.M) {
	// Uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// Uses pool to try to connect to Docker
	err = pool.Client.Ping()
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}

	// Pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "latest",
		Env: []string{
			// username and password for mongodb superuser
			"MONGO_INITDB_ROOT_USERNAME=root",
			"MONGO_INITDB_ROOT_PASSWORD=password",
		},
	}, func(config *docker.HostConfig) {
		// set AutoRemove to true so that stopped container goes away by itself
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	err = pool.Retry(func() error {
		var err error
		client, err = mongox.Connect(
			ctx,
			mongox.Config{
				AppName: "mongox-test",
				Hosts: []string{
					"localhost:" + resource.GetPort("27017/tcp"),
				},
				Compressors: []string{
					"snappy",
				},
				Connection: &mongox.ConnectionConfig{
					ConnectTimeout:  lang.Ptr(10 * time.Second),
					MaxConnIdleTime: lang.Ptr(10 * time.Second),
					MaxConnecting:   lang.Ptr(uint64(10)),
					MaxPoolSize:     lang.Ptr(uint64(10)),
					MinPoolSize:     lang.Ptr(uint64(1)),
					IsDirect:        true,
				},
				Auth: &mongox.AuthConfig{
					Username:      "root",
					Password:      "password",
					AuthMechanism: "SCRAM-SHA-256",
				},
				BSONOptions: &mongox.BSONOptions{
					ErrorOnInlineDuplicates: true, // test buildBSONOptions
				},
			},
		)
		if err != nil {
			return err
		}
		return client.Ping(ctx)
	})
	if err != nil {
		log.Fatalf("Could not connect to mongo container: %s", err)
	}

	defer func() {
		// When you're done, kill and remove the container
		if err = pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}

		// disconnect mongodb client
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// run tests
	m.Run()
}

func testFindOne(t *testing.T, ctx context.Context, db *mongox.Database, entity testEntity, filter mongox.M, err2 ...error) {
	testOne(t, ctx, db.Collection(findOneCollection), entity, filter, err2...)
}

func testUpdate(t *testing.T, ctx context.Context, db *mongox.Database, entity testEntity, filter mongox.M, err2 ...error) {
	testOne(t, ctx, db.Collection(updateCollection), entity, filter, err2...)
}

func testBulk(t *testing.T, ctx context.Context, db *mongox.Database, entity testEntity, filter mongox.M, err2 ...error) {
	testOne(t, ctx, db.Collection(bulkCollection), entity, filter, err2...)
}

func testAsync(t *testing.T, ctx context.Context, db *mongox.Database, entity testEntity, filter mongox.M, err2 ...error) {
	testOne(t, ctx, db.Collection(asyncCollection), entity, filter, err2...)
}

func testOne(t *testing.T, ctx context.Context, coll *mongox.Collection, entity testEntity, filter mongox.M, err2 ...error) {
	var result testEntity
	err := coll.FindOne(ctx, &result, filter)
	if len(err2) > 0 {
		if !errors.Is(err, err2[0]) {
			t.Errorf("expected %v, got %v, query: %v", err2[0], err, filter)
		}
		return
	}
	if err != nil {
		t.Errorf("expected nil, got %v, query: %v", err, filter)
		return
	}
	if !reflect.DeepEqual(entity, result) {
		t.Errorf("expected %v, got %v, query: %v", entity, result, filter)
	}
}

func testFind(t *testing.T, ctx context.Context, db *mongox.Database, entity []testEntity, filter mongox.M, err2 ...error) {
	var result []testEntity
	err := db.Collection(findCollection).Find(ctx, &result, filter)
	if len(err2) > 0 {
		if !errors.Is(err, err2[0]) {
			t.Errorf("expected %v, got %v, query: %v", err2[0], err, filter)
		}
		return
	}
	if err != nil {
		t.Errorf("expected nil, got %v, query: %v", err, filter)
		return
	}
	if len(result) != len(entity) {
		t.Errorf("expected %d, got %d, query: %v", len(entity), len(result), filter)
	}
	if len(entity) == 0 {
		return
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	sort.Slice(entity, func(i, j int) bool {
		return entity[i].ID < entity[j].ID
	})
	if !reflect.DeepEqual(entity, result) {
		t.Errorf("expected %v, got %v, query: %v", entity, &result, filter)
	}
}

type testEntity struct {
	ID           string         `bson:"id"`
	Name         string         `bson:"name,omitempty"`
	Number       int            `bson:"number,omitempty"`
	Bool         bool           `bson:"bool,omitempty"`
	Slice        []int          `bson:"slice,omitempty"`
	Array        [3]int         `bson:"array,omitempty"`
	Map          map[string]int `bson:"map,omitempty"`
	Time         time.Time      `bson:"time,omitempty"`
	Struct       innerStruct    `bson:"struct,omitempty"`
	InlineStruct inlineStruct   `bson:"inline_struct,inline"`
}

type innerStruct struct {
	Name   string `bson:"name"`
	Number int    `bson:"number"`
}

type inlineStruct struct {
	InlineField string `bson:"inline_field"`
}

func newTestEntity(id string) testEntity {
	randString := func() string {
		b := make([]byte, 10)
		for i := range b {
			b[i] = byte(rand.Intn(26) + 'a')
		}
		return string(b)
	}
	return testEntity{
		ID:     id,
		Name:   randString(),
		Number: rand.Intn(math.MaxInt32) - 1,
		Bool:   true,
		Slice:  []int{rand.Intn(26), rand.Intn(26), rand.Intn(26)},
		Array:  [3]int{rand.Intn(26), rand.Intn(26), rand.Intn(26)},
		Map:    map[string]int{"1": rand.Intn(26), randString(): rand.Intn(26), randString(): rand.Intn(26)},
		Time:   time.Date(2000, 1, rand.Intn(26), rand.Intn(20), rand.Intn(59), rand.Intn(59), 0, time.UTC),
		Struct: innerStruct{Name: randString(), Number: rand.Intn(26)},
	}
}

type testUpdateEntity struct {
	name string
}

// func (t testUpdateEntity) MarshalBSONValue() (bson.Type, []byte, error) {
// 	return bson.MarshalValue(t.name)
// }

// func (t *testUpdateEntity) UnmarshalBSONValue(typ bson.Type, data []byte) error {
// 	return bson.UnmarshalValue(typ, data, &t.name)
// }

func (t testUpdateEntity) MarshalBSONValue() (byte, []byte, error) {
	typ, data, err := bson.MarshalValue(t.name)
	return byte(typ), data, err
}

func (t *testUpdateEntity) UnmarshalBSONValue(typ byte, data []byte) error {
	return bson.UnmarshalValue(bson.Type(typ), data, &t.name)
}

func TestFindOneMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)
	coll := db.Collection("find_one_methods_test")

	// Setup test data
	entities := make([]testEntity, 5)
	for i := 0; i < 5; i++ {
		entities[i] = newTestEntity(fmt.Sprintf("%d", i+1))
	}

	// Insert test data
	_, err := coll.Insert(ctx,
		entities[0], entities[1], entities[2], entities[3], entities[4])
	if err != nil {
		t.Fatal(err)
	}

	t.Run("FindOne_BasicFilters", func(t *testing.T) {
		// Test FindOne with basic filters
		var result testEntity
		err := coll.FindOne(ctx, &result, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" {
			t.Errorf("expected ID '1', got '%s'", result.ID)
		}

		// Test FindOne with complex filter
		err = coll.FindOne(ctx, &result, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "2" && result.ID != "3" {
			t.Errorf("expected ID '2' or '3', got '%s'", result.ID)
		}

		// Test FindOne with no filter (should return any document)
		err = coll.FindOne(ctx, &result, nil)
		if err != nil {
			t.Error(err)
		}
		if result.ID == "" {
			t.Error("expected non-empty ID")
		}
	})

	t.Run("FindOne_WithOptions", func(t *testing.T) {
		var result testEntity

		// Test FindOne with Skip option
		err := coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			Skip: 2,
			Sort: mongox.M{"id": 1},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "3" {
			t.Errorf("expected ID '3' with skip=2, got '%s'", result.ID)
		}

		// Test FindOne with Sort option (ascending)
		err = coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			Sort: mongox.M{"id": mongox.Ascending},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" {
			t.Errorf("expected ID '1' with ascending sort, got '%s'", result.ID)
		}

		// Test FindOne with Sort option (descending)
		err = coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			Sort: mongox.M{"id": mongox.Descending},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "5" {
			t.Errorf("expected ID '5' with descending sort, got '%s'", result.ID)
		}

		// Test FindOne with SortMany option
		err = coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			SortMany: []mongox.M{
				{"id": mongox.Descending},
				{"name": mongox.Ascending},
			},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "5" {
			t.Errorf("expected ID '5' with SortMany descending, got '%s'", result.ID)
		}

		// Test that Sort has priority over SortMany
		err = coll.FindOne(ctx, &result, nil, mongox.FindOptions{
			Sort: mongox.M{"id": mongox.Ascending},
			SortMany: []mongox.M{
				{"id": mongox.Descending},
			},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" {
			t.Errorf("expected ID '1' (Sort should override SortMany), got '%s'", result.ID)
		}

		// Test FindOne with AllowPartialResults option
		err = coll.FindOne(ctx, &result, mongox.M{"id": "1"}, mongox.FindOptions{
			AllowPartialResults: true,
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" {
			t.Errorf("expected ID '1' with AllowPartialResults, got '%s'", result.ID)
		}
	})

	t.Run("FindOne_ErrorCases", func(t *testing.T) {
		var result testEntity

		// Test FindOne with non-existent document
		err := coll.FindOne(ctx, &result, mongox.M{"id": "999"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}

		// Test FindOne with invalid filter that won't match anything
		err = coll.FindOne(ctx, &result, mongox.M{"nonexistent_field": "value"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-matching filter, got %v", err)
		}

		// Test FindOne with nil destination (should error)
		err = coll.FindOne(ctx, nil, mongox.M{"id": "1"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for nil destination, got %v", err)
		}

		// Test FindOne with non-pointer destination (should error)
		err = coll.FindOne(ctx, result, mongox.M{"id": "1"})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for non-pointer destination, got %v", err)
		}
	})

	t.Run("Generic_FindOne", func(t *testing.T) {
		// Test generic FindOne[T] method
		result, err := mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" {
			t.Errorf("expected ID '1', got '%s'", result.ID)
		}

		// Test generic FindOne[T] with options
		result, err = mongox.FindOne[testEntity](ctx, coll, nil, mongox.FindOptions{
			Sort: mongox.M{"id": mongox.Descending},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "5" {
			t.Errorf("expected ID '5' with descending sort, got '%s'", result.ID)
		}

		// Test generic FindOne[T] with skip and sort
		result, err = mongox.FindOne[testEntity](ctx, coll, nil, mongox.FindOptions{
			Skip: 1,
			Sort: mongox.M{"id": mongox.Ascending},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "2" {
			t.Errorf("expected ID '2' with skip=1 and ascending sort, got '%s'", result.ID)
		}

		// Test generic FindOne[T] error case
		_, err = mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "999"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("FindOne_ComplexFilters", func(t *testing.T) {
		var result testEntity

		// Test FindOne with $and operator
		err := coll.FindOne(ctx, &result, mongox.M{
			mongox.And: []mongox.M{
				{"id": mongox.M{mongox.Gte: "2"}},
				{"id": mongox.M{mongox.Lte: "3"}},
			},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "2" && result.ID != "3" {
			t.Errorf("expected ID '2' or '3', got '%s'", result.ID)
		}

		// Test FindOne with $or operator
		err = coll.FindOne(ctx, &result, mongox.M{
			mongox.Or: []mongox.M{
				{"id": "1"},
				{"id": "5"},
			},
		})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "1" && result.ID != "5" {
			t.Errorf("expected ID '1' or '5', got '%s'", result.ID)
		}

		// Test FindOne with nested field
		err = coll.FindOne(ctx, &result, mongox.M{
			"struct.name": entities[0].Struct.Name,
		})
		if err != nil {
			t.Error(err)
		}
		if result.Struct.Name != entities[0].Struct.Name {
			t.Errorf("expected struct name '%s', got '%s'", entities[0].Struct.Name, result.Struct.Name)
		}

		// Test FindOne with array field
		err = coll.FindOne(ctx, &result, mongox.M{
			"slice": entities[1].Slice,
		})
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(result.Slice, entities[1].Slice) {
			t.Errorf("expected slice %v, got %v", entities[1].Slice, result.Slice)
		}
	})

	t.Run("FindOne_FieldProjection", func(t *testing.T) {
		// MongoDB doesn't support field projection directly in FindOne options in this wrapper,
		// but we can test that we get full documents
		var result testEntity
		err := coll.FindOne(ctx, &result, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}

		// Verify all fields are populated
		if result.ID == "" {
			t.Error("ID should not be empty")
		}
		if result.Name == "" {
			t.Error("Name should not be empty")
		}
		if result.Number == 0 {
			t.Error("Number should not be zero")
		}
		if len(result.Slice) == 0 {
			t.Error("Slice should not be empty")
		}
	})

	t.Run("FindOne_TypeSafety", func(t *testing.T) {
		// Test that FindOne properly handles different destination types

		// Test with map destination
		var mapResult map[string]interface{}
		err := coll.FindOne(ctx, &mapResult, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if mapResult["id"] != "1" {
			t.Errorf("expected map ID '1', got '%v'", mapResult["id"])
		}

		// Test with bson.M destination
		var bsonResult bson.M
		err = coll.FindOne(ctx, &bsonResult, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}
		if bsonResult["id"] != "1" {
			t.Errorf("expected bson.M ID '1', got '%v'", bsonResult["id"])
		}
	})

	// Cleanup
	_, err = coll.DeleteMany(ctx, nil)
	if err != nil {
		t.Error(err)
	}
}

func TestFindOneAndMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)
	coll := db.Collection("find_one_and_methods_test")

	t.Run("FindOneAndDelete", func(t *testing.T) {
		// Setup test data
		entity1 := newTestEntity("delete1")
		entity2 := newTestEntity("delete2")
		entity3 := newTestEntity("delete3")

		_, err := coll.Insert(ctx, entity1, entity2, entity3)
		if err != nil {
			t.Fatal(err)
		}

		// Test FindOneAndDelete - should return the deleted document
		var deletedEntity testEntity
		err = coll.FindOneAndDelete(ctx, &deletedEntity, mongox.M{"id": "delete1"})
		if err != nil {
			t.Error(err)
		}
		if deletedEntity.ID != "delete1" {
			t.Errorf("expected deleted entity ID 'delete1', got '%s'", deletedEntity.ID)
		}
		if deletedEntity.Name != entity1.Name {
			t.Errorf("expected deleted entity name '%s', got '%s'", entity1.Name, deletedEntity.Name)
		}

		// Verify the document was actually deleted
		var checkEntity testEntity
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "delete1"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound after deletion, got %v", err)
		}

		// Test generic FindOneAndDelete[T]
		deletedEntity2, err := mongox.FindOneAndDelete[testEntity](ctx, coll, mongox.M{"id": "delete2"})
		if err != nil {
			t.Error(err)
		}
		if deletedEntity2.ID != "delete2" {
			t.Errorf("expected deleted entity ID 'delete2', got '%s'", deletedEntity2.ID)
		}

		// Verify the document was actually deleted
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "delete2"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound after deletion, got %v", err)
		}

		// Test FindOneAndDelete with non-existent document
		var nonExistentEntity testEntity
		err = coll.FindOneAndDelete(ctx, &nonExistentEntity, mongox.M{"id": "nonexistent"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Test generic FindOneAndDelete[T] with non-existent document
		_, err = mongox.FindOneAndDelete[testEntity](ctx, coll, mongox.M{"id": "nonexistent"})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Test FindOneAndDelete with complex filter
		err = coll.FindOneAndDelete(ctx, &deletedEntity, mongox.M{
			"id": mongox.M{mongox.In: []string{"delete3", "delete4"}},
		})
		if err != nil {
			t.Error(err)
		}
		if deletedEntity.ID != "delete3" {
			t.Errorf("expected deleted entity ID 'delete3', got '%s'", deletedEntity.ID)
		}

		// Cleanup remaining test data
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.Regex: "delete"}})
	})

	t.Run("FindOneAndReplace", func(t *testing.T) {
		// Setup test data
		originalEntity := newTestEntity("replace1")
		entity2 := newTestEntity("replace2")

		_, err := coll.Insert(ctx, originalEntity, entity2)
		if err != nil {
			t.Fatal(err)
		}

		// Create replacement entity
		replacementEntity := newTestEntity("replace1") // Same ID but different data
		replacementEntity.Name = "replaced-name"
		replacementEntity.Number = 99999

		// Test FindOneAndReplace - should return the original document
		var returnedEntity testEntity
		err = coll.FindOneAndReplace(ctx, &returnedEntity, mongox.M{"id": "replace1"}, replacementEntity)
		if err != nil {
			t.Error(err)
		}
		if returnedEntity.ID != "replace1" {
			t.Errorf("expected returned entity ID 'replace1', got '%s'", returnedEntity.ID)
		}
		if returnedEntity.Name != originalEntity.Name {
			t.Errorf("expected returned entity to be original, got name '%s', expected '%s'", returnedEntity.Name, originalEntity.Name)
		}

		// Verify the document was actually replaced
		var checkEntity testEntity
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "replace1"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Name != "replaced-name" {
			t.Errorf("expected replaced entity name 'replaced-name', got '%s'", checkEntity.Name)
		}
		if checkEntity.Number != 99999 {
			t.Errorf("expected replaced entity number 99999, got %d", checkEntity.Number)
		}

		// Test generic FindOneAndReplace[T]
		anotherReplacement := newTestEntity("replace2")
		anotherReplacement.Name = "another-replaced-name"

		returnedEntity2, err := mongox.FindOneAndReplace[testEntity](ctx, coll, mongox.M{"id": "replace2"}, anotherReplacement)
		if err != nil {
			t.Error(err)
		}
		if returnedEntity2.ID != "replace2" {
			t.Errorf("expected returned entity ID 'replace2', got '%s'", returnedEntity2.ID)
		}
		if returnedEntity2.Name == "another-replaced-name" {
			t.Error("returned entity should be the original, not the replacement")
		}

		// Verify the document was actually replaced
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "replace2"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Name != "another-replaced-name" {
			t.Errorf("expected replaced entity name 'another-replaced-name', got '%s'", checkEntity.Name)
		}

		// Test FindOneAndReplace with non-existent document
		var nonExistentEntity testEntity
		err = coll.FindOneAndReplace(ctx, &nonExistentEntity, mongox.M{"id": "nonexistent"}, newTestEntity("nonexistent"))
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Test generic FindOneAndReplace[T] with non-existent document
		_, err = mongox.FindOneAndReplace[testEntity](ctx, coll, mongox.M{"id": "nonexistent"}, newTestEntity("nonexistent"))
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Cleanup test data
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.Regex: "replace"}})
	})

	t.Run("FindOneAndUpdate", func(t *testing.T) {
		// Setup test data
		originalEntity := newTestEntity("update1")
		entity2 := newTestEntity("update2")
		entity3 := newTestEntity("update3")

		_, err := coll.Insert(ctx, originalEntity, entity2, entity3)
		if err != nil {
			t.Fatal(err)
		}

		// Test FindOneAndUpdate with $set operator - should return original document
		var returnedEntity testEntity
		err = coll.FindOneAndUpdate(ctx, &returnedEntity, mongox.M{"id": "update1"}, mongox.M{
			mongox.Set: mongox.M{
				"name":   "updated-name",
				"number": 12345,
			},
		})
		if err != nil {
			t.Error(err)
		}
		if returnedEntity.ID != "update1" {
			t.Errorf("expected returned entity ID 'update1', got '%s'", returnedEntity.ID)
		}
		if returnedEntity.Name != originalEntity.Name {
			t.Errorf("expected returned entity to be original, got name '%s', expected '%s'", returnedEntity.Name, originalEntity.Name)
		}

		// Verify the document was actually updated
		var checkEntity testEntity
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "update1"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Name != "updated-name" {
			t.Errorf("expected updated entity name 'updated-name', got '%s'", checkEntity.Name)
		}
		if checkEntity.Number != 12345 {
			t.Errorf("expected updated entity number 12345, got %d", checkEntity.Number)
		}

		// Test FindOneAndUpdate with $inc operator
		err = coll.FindOneAndUpdate(ctx, &returnedEntity, mongox.M{"id": "update1"}, mongox.M{
			mongox.Inc: mongox.M{"number": 100},
		})
		if err != nil {
			t.Error(err)
		}
		if returnedEntity.Number != 12345 {
			t.Errorf("expected returned entity to have original number 12345, got %d", returnedEntity.Number)
		}

		// Verify the increment was applied
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "update1"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Number != 12445 {
			t.Errorf("expected incremented number 12445, got %d", checkEntity.Number)
		}

		// Test generic FindOneAndUpdate[T]
		returnedEntity2, err := mongox.FindOneAndUpdate[testEntity](ctx, coll, mongox.M{"id": "update2"}, mongox.M{
			mongox.Set: mongox.M{"name": "generic-updated-name"},
		})
		if err != nil {
			t.Error(err)
		}
		if returnedEntity2.ID != "update2" {
			t.Errorf("expected returned entity ID 'update2', got '%s'", returnedEntity2.ID)
		}
		if returnedEntity2.Name == "generic-updated-name" {
			t.Error("returned entity should be the original, not the updated version")
		}

		// Verify the document was actually updated
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "update2"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Name != "generic-updated-name" {
			t.Errorf("expected updated entity name 'generic-updated-name', got '%s'", checkEntity.Name)
		}

		// Test FindOneAndUpdate with complex update using multiple operators
		err = coll.FindOneAndUpdate(ctx, &returnedEntity, mongox.M{"id": "update3"}, mongox.M{
			mongox.Set:  mongox.M{"name": "complex-updated"},
			mongox.Inc:  mongox.M{"number": 500},
			mongox.Push: mongox.M{"slice": 999},
		})
		if err != nil {
			t.Error(err)
		}

		// Verify complex update was applied
		err = coll.FindOne(ctx, &checkEntity, mongox.M{"id": "update3"})
		if err != nil {
			t.Error(err)
		}
		if checkEntity.Name != "complex-updated" {
			t.Errorf("expected updated entity name 'complex-updated', got '%s'", checkEntity.Name)
		}
		if checkEntity.Number != entity3.Number+500 {
			t.Errorf("expected incremented number %d, got %d", entity3.Number+500, checkEntity.Number)
		}
		found999 := false
		for _, val := range checkEntity.Slice {
			if val == 999 {
				found999 = true
				break
			}
		}
		if !found999 {
			t.Error("expected 999 to be pushed to slice")
		}

		// Test FindOneAndUpdate with non-existent document
		var nonExistentEntity testEntity
		err = coll.FindOneAndUpdate(ctx, &nonExistentEntity, mongox.M{"id": "nonexistent"}, mongox.M{
			mongox.Set: mongox.M{"name": "wont-work"},
		})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Test generic FindOneAndUpdate[T] with non-existent document
		_, err = mongox.FindOneAndUpdate[testEntity](ctx, coll, mongox.M{"id": "nonexistent"}, mongox.M{
			mongox.Set: mongox.M{"name": "wont-work"},
		})
		if !errors.Is(err, mongox.ErrNotFound) {
			t.Errorf("expected ErrNotFound for non-existent document, got %v", err)
		}

		// Test FindOneAndUpdate with invalid update document
		err = coll.FindOneAndUpdate(ctx, &returnedEntity, mongox.M{"id": "update1"}, mongox.M{
			"invalid": "no-dollar-operator",
		})
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for invalid update document, got %v", err)
		}

		// Cleanup test data
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.Regex: "update"}})
	})

	t.Run("FindOneAnd_ErrorHandling", func(t *testing.T) {
		// Test error handling for all FindOneAnd methods

		// Test with nil destination - should get ErrNotFound since document doesn't exist
		err := coll.FindOneAndDelete(ctx, nil, mongox.M{"id": "test"})
		if err == nil {
			t.Error("expected error for nil destination in FindOneAndDelete")
		}

		err = coll.FindOneAndReplace(ctx, nil, mongox.M{"id": "test"}, newTestEntity("test"))
		if err == nil {
			t.Error("expected error for nil destination in FindOneAndReplace")
		}

		err = coll.FindOneAndUpdate(ctx, nil, mongox.M{"id": "test"}, mongox.M{mongox.Set: mongox.M{"name": "test"}})
		if err == nil {
			t.Error("expected error for nil destination in FindOneAndUpdate")
		}

		// Test with non-pointer destination - should get error
		var entity testEntity
		err = coll.FindOneAndDelete(ctx, entity, mongox.M{"id": "test"})
		if err == nil {
			t.Error("expected error for non-pointer destination in FindOneAndDelete")
		}

		err = coll.FindOneAndReplace(ctx, entity, mongox.M{"id": "test"}, newTestEntity("test"))
		if err == nil {
			t.Error("expected error for non-pointer destination in FindOneAndReplace")
		}

		err = coll.FindOneAndUpdate(ctx, entity, mongox.M{"id": "test"}, mongox.M{mongox.Set: mongox.M{"name": "test"}})
		if err == nil {
			t.Error("expected error for non-pointer destination in FindOneAndUpdate")
		}
	})

	t.Run("FindOneAnd_TypeSafety", func(t *testing.T) {
		// Setup test data
		testEntity := newTestEntity("typesafety")
		_, err := coll.Insert(ctx, testEntity)
		if err != nil {
			t.Fatal(err)
		}

		// Test with map destination
		var mapResult map[string]interface{}
		err = coll.FindOneAndDelete(ctx, &mapResult, mongox.M{"id": "typesafety"})
		if err != nil {
			t.Error(err)
		}
		if mapResult["id"] != "typesafety" {
			t.Errorf("expected map ID 'typesafety', got '%v'", mapResult["id"])
		}

		// Re-insert for next test
		_, err = coll.Insert(ctx, testEntity)
		if err != nil {
			t.Fatal(err)
		}

		// Test with bson.M destination
		var bsonResult bson.M
		replacementMap := map[string]interface{}{
			"id":     "typesafety",
			"name":   "replaced-via-map",
			"number": 777,
		}
		err = coll.FindOneAndReplace(ctx, &bsonResult, mongox.M{"id": "typesafety"}, replacementMap)
		if err != nil {
			t.Error(err)
		}
		if bsonResult["id"] != "typesafety" {
			t.Errorf("expected bson.M ID 'typesafety', got '%v'", bsonResult["id"])
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": "typesafety"})
	})

	// Final cleanup
	_, err := coll.DeleteMany(ctx, nil)
	if err != nil && !errors.Is(err, mongox.ErrNotFound) {
		t.Error(err)
	}
}

func TestInsertMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := client.Database(dbName)
	coll := db.Collection("insert_methods_test")

	t.Run("InsertOne_Basic", func(t *testing.T) {
		// Test basic InsertOne functionality
		entity := newTestEntity("insertone1")

		id, err := coll.InsertOne(ctx, entity)
		if err != nil {
			t.Error(err)
		}
		if id.IsZero() {
			t.Error("expected non-zero ObjectID")
		}

		// Verify the document was inserted
		var result testEntity
		err = coll.FindOne(ctx, &result, mongox.M{"id": "insertone1"})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "insertone1" {
			t.Errorf("expected ID 'insertone1', got '%s'", result.ID)
		}

		// Test generic InsertOne
		entity2 := newTestEntity("insertone2")
		id2, err := mongox.InsertOne(ctx, coll, entity2)
		if err != nil {
			t.Error(err)
		}
		if id2.IsZero() {
			t.Error("expected non-zero ObjectID from generic InsertOne")
		}

		// Verify the document was inserted
		err = coll.FindOne(ctx, &result, mongox.M{"id": "insertone2"})
		if err != nil {
			t.Error(err)
		}
		if result.ID != "insertone2" {
			t.Errorf("expected ID 'insertone2', got '%s'", result.ID)
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"insertone1", "insertone2"}}})
	})

	t.Run("InsertOne_StrictID", func(t *testing.T) {
		// Test InsertOne with strict ID validation enabled
		entity := newTestEntity("strictone1")

		id, err := coll.InsertOne(ctx, entity, true)
		if err != nil {
			t.Error(err)
		}
		if id.IsZero() {
			t.Error("expected non-zero ObjectID with strict validation")
		}

		// Test generic InsertOne with strict validation
		entity2 := newTestEntity("strictone2")
		id2, err := mongox.InsertOne(ctx, coll, entity2, true)
		if err != nil {
			t.Error(err)
		}
		if id2.IsZero() {
			t.Error("expected non-zero ObjectID from generic InsertOne with strict validation")
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"strictone1", "strictone2"}}})
	})

	t.Run("InsertOne_WithCustomID", func(t *testing.T) {
		// Test InsertOne with a document that has a custom string ID
		// This should succeed in non-strict mode but return empty ObjectID
		entityWithCustomID := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "custom-string-id",
			Name: "test-entity",
		}

		id, err := coll.InsertOne(ctx, entityWithCustomID)
		if err != nil {
			t.Error(err)
		}
		if !id.IsZero() {
			t.Error("expected zero ObjectID when using custom string ID")
		}

		// Test with strict validation - should fail
		entityWithCustomID2 := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "custom-string-id-2",
			Name: "test-entity-2",
		}

		_, err = coll.InsertOne(ctx, entityWithCustomID2, true)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument with strict validation and custom string ID, got %v", err)
		}

		// Cleanup
		_ = coll.DeleteOne(ctx, mongox.M{"_id": "custom-string-id"})
	})

	t.Run("InsertOne_ErrorHandling", func(t *testing.T) {
		// Test InsertOne with nil record
		_, err := coll.InsertOne(ctx, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for nil record, got %v", err)
		}

		// Test InsertOne with invalid record type
		_, err = coll.InsertOne(ctx, func() {})
		if err == nil {
			t.Error("expected error for invalid record type")
		}

		// Test duplicate key error with custom _id field
		entityWithFixedID := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "fixed-duplicate-test",
			Name: "test-entity",
		}

		_, err = coll.InsertOne(ctx, entityWithFixedID)
		if err != nil {
			t.Error(err)
		}

		// Try to insert the same entity again (should fail due to duplicate _id)
		_, err = coll.InsertOne(ctx, entityWithFixedID)
		if !errors.Is(err, mongox.ErrDuplicate) {
			t.Errorf("expected ErrDuplicate for duplicate _id, got %v", err)
		}

		// Cleanup
		_ = coll.DeleteOne(ctx, mongox.M{"_id": "fixed-duplicate-test"})
	})

	t.Run("Insert_Basic", func(t *testing.T) {
		// Test basic Insert functionality (non-strict by default)
		entity1 := newTestEntity("insert1")
		entity2 := newTestEntity("insert2")
		entity3 := newTestEntity("insert3")

		ids, err := coll.Insert(ctx, entity1, entity2, entity3)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 3 {
			t.Errorf("expected 3 IDs, got %d", len(ids))
		}
		for i, id := range ids {
			if id.IsZero() {
				t.Errorf("expected non-zero ObjectID at index %d", i)
			}
		}

		// Verify all documents were inserted
		var results []testEntity
		err = coll.Find(ctx, &results, mongox.M{"id": mongox.M{mongox.In: []string{"insert1", "insert2", "insert3"}}})
		if err != nil {
			t.Error(err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 documents found, got %d", len(results))
		}

		// Test generic Insert
		entity4 := newTestEntity("insert4")
		entity5 := newTestEntity("insert5")

		ids2, err := mongox.Insert(ctx, coll, entity4, entity5)
		if err != nil {
			t.Error(err)
		}
		if len(ids2) != 2 {
			t.Errorf("expected 2 IDs from generic Insert, got %d", len(ids2))
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"insert1", "insert2", "insert3", "insert4", "insert5"}}})
	})

	t.Run("Insert_WithCustomIDs", func(t *testing.T) {
		// Test Insert with custom string IDs (non-strict mode)
		entitiesWithCustomIDs := []any{
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "custom-1",
				Name: "entity-1",
			},
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "custom-2",
				Name: "entity-2",
			},
		}

		ids, err := coll.Insert(ctx, entitiesWithCustomIDs...)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 IDs, got %d", len(ids))
		}
		// With custom string IDs, the returned ObjectIDs should be zero
		for i, id := range ids {
			if !id.IsZero() {
				t.Errorf("expected zero ObjectID at index %d when using custom string ID", i)
			}
		}

		// Verify documents were inserted
		count, err := coll.Count(ctx, mongox.M{"_id": mongox.M{mongox.In: []string{"custom-1", "custom-2"}}})
		if err != nil {
			t.Error(err)
		}
		if count != 2 {
			t.Errorf("expected 2 documents with custom IDs, got %d", count)
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"_id": mongox.M{mongox.In: []string{"custom-1", "custom-2"}}})
	})

	t.Run("InsertStrict_Basic", func(t *testing.T) {
		// Test InsertStrict functionality
		entity1 := newTestEntity("strict1")
		entity2 := newTestEntity("strict2")

		ids, err := coll.InsertStrict(ctx, entity1, entity2)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 IDs from InsertStrict, got %d", len(ids))
		}
		for i, id := range ids {
			if id.IsZero() {
				t.Errorf("expected non-zero ObjectID at index %d from InsertStrict", i)
			}
		}

		// Test generic InsertStrict
		entity3 := newTestEntity("strict3")
		ids2, err := mongox.InsertStrict(ctx, coll, entity3)
		if err != nil {
			t.Error(err)
		}
		if len(ids2) != 1 {
			t.Errorf("expected 1 ID from generic InsertStrict, got %d", len(ids2))
		}
		if ids2[0].IsZero() {
			t.Error("expected non-zero ObjectID from generic InsertStrict")
		}

		// Verify all documents were inserted
		var results []testEntity
		err = coll.Find(ctx, &results, mongox.M{"id": mongox.M{mongox.In: []string{"strict1", "strict2", "strict3"}}})
		if err != nil {
			t.Error(err)
		}
		if len(results) != 3 {
			t.Errorf("expected 3 documents found, got %d", len(results))
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"strict1", "strict2", "strict3"}}})
	})

	t.Run("InsertStrict_WithCustomIDs", func(t *testing.T) {
		// First cleanup any potentially existing documents with these IDs
		_, _ = coll.DeleteMany(ctx, mongox.M{"_id": mongox.M{mongox.In: []string{"strict-custom-unique-1", "strict-custom-unique-2"}}})

		// Test InsertStrict with custom string IDs - should fail
		entitiesWithCustomIDs := []any{
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "strict-custom-unique-1",
				Name: "entity-1",
			},
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "strict-custom-unique-2",
				Name: "entity-2",
			},
		}

		_, err := coll.InsertStrict(ctx, entitiesWithCustomIDs...)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument with InsertStrict and custom string IDs, got %v", err)
		}

		// Test generic InsertStrict with custom ID (use a different ID)
		singleEntityWithCustomID := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "strict-generic-unique-1",
			Name: "entity-generic",
		}

		_, err = mongox.InsertStrict(ctx, coll, singleEntityWithCustomID)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument with generic InsertStrict and custom string ID, got %v", err)
		}
	})

	t.Run("Insert_ErrorHandling", func(t *testing.T) {
		// Test Insert with nil records
		_, err := coll.Insert(ctx)
		if err != nil {
			t.Error("Insert with no records should not error")
		}

		_, err = coll.Insert(ctx, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for nil record in Insert, got %v", err)
		}

		// Test Insert with mixed valid and invalid records
		entity := newTestEntity("mixed")
		_, err = coll.Insert(ctx, entity, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for mixed valid/nil records, got %v", err)
		}

		// Test InsertStrict with nil records
		_, err = coll.InsertStrict(ctx, nil)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument for nil record in InsertStrict, got %v", err)
		}
	})

	t.Run("StrictID_Comparison", func(t *testing.T) {
		// Compare behavior between strict and non-strict modes

		// Create an entity with custom ID
		entityWithCustomID := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "comparison-test",
			Name: "test-entity",
		}

		// Non-strict mode should succeed and return zero ObjectID
		ids, err := coll.Insert(ctx, entityWithCustomID)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 1 || !ids[0].IsZero() {
			t.Error("non-strict Insert should return zero ObjectID for custom string ID")
		}

		// Cleanup before next test
		_ = coll.DeleteOne(ctx, mongox.M{"_id": "comparison-test"})

		// Strict mode should fail
		_, err = coll.InsertStrict(ctx, entityWithCustomID)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("strict Insert should fail with custom string ID, got error: %v", err)
		}

		// Test with regular testEntity (should work in both modes)
		entity := newTestEntity("comparison-regular")

		// Non-strict mode
		ids, err = coll.Insert(ctx, entity)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 1 || ids[0].IsZero() {
			t.Error("non-strict Insert should return non-zero ObjectID for regular entity")
		}

		// Cleanup
		_ = coll.DeleteOne(ctx, mongox.M{"id": "comparison-regular"})

		// Strict mode
		ids, err = coll.InsertStrict(ctx, entity)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 1 || ids[0].IsZero() {
			t.Error("strict Insert should return non-zero ObjectID for regular entity")
		}

		// Cleanup
		_ = coll.DeleteOne(ctx, mongox.M{"id": "comparison-regular"})
	})

	t.Run("InsertMany_StrictID", func(t *testing.T) {
		// Test InsertMany with strict ID parameter
		entities := []any{
			newTestEntity("many1"),
			newTestEntity("many2"),
			newTestEntity("many3"),
		}

		// Test InsertMany with strict=false
		ids, err := coll.InsertMany(ctx, entities, false)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 3 {
			t.Errorf("expected 3 IDs from InsertMany, got %d", len(ids))
		}
		for i, id := range ids {
			if id.IsZero() {
				t.Errorf("expected non-zero ObjectID at index %d from InsertMany", i)
			}
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"many1", "many2", "many3"}}})

		// Test InsertMany with strict=true
		ids, err = coll.InsertMany(ctx, entities, true)
		if err != nil {
			t.Error(err)
		}
		if len(ids) != 3 {
			t.Errorf("expected 3 IDs from strict InsertMany, got %d", len(ids))
		}
		for i, id := range ids {
			if id.IsZero() {
				t.Errorf("expected non-zero ObjectID at index %d from strict InsertMany", i)
			}
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"many1", "many2", "many3"}}})

		// Test InsertMany with custom IDs and strict=true (should fail)
		entitiesWithCustomIDs := []any{
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "many-custom-unique-1",
				Name: "entity-1",
			},
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "many-custom-unique-2",
				Name: "entity-2",
			},
		}

		_, err = coll.InsertMany(ctx, entitiesWithCustomIDs, true)
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected ErrInvalidArgument with strict InsertMany and custom string IDs, got %v", err)
		}

		// Create new entities with different IDs for the non-strict test
		entitiesWithCustomIDs2 := []any{
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "many-custom-nonstrict-1",
				Name: "entity-nonstrict-1",
			},
			struct {
				ID   string `bson:"_id"`
				Name string `bson:"name"`
			}{
				ID:   "many-custom-nonstrict-2",
				Name: "entity-nonstrict-2",
			},
		}

		// Test InsertMany with custom IDs and strict=false (should succeed)
		ids, err = coll.InsertMany(ctx, entitiesWithCustomIDs2, false)
		if err != nil {
			t.Errorf("non-strict InsertMany with custom IDs failed: %v", err)
		}
		if len(ids) != 2 {
			t.Errorf("expected 2 IDs from non-strict InsertMany with custom IDs, got %d", len(ids))
		}
		for i, id := range ids {
			if !id.IsZero() {
				t.Errorf("expected zero ObjectID at index %d for custom string ID", i)
			}
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"_id": mongox.M{mongox.In: []string{"many-custom-nonstrict-1", "many-custom-nonstrict-2"}}})
	})

	t.Run("BulkWrite_WithStrictID", func(t *testing.T) {
		// Test bulk operations and verify ID handling
		bulker := mongox.NewBulkBuilder()

		// Add various insert operations
		entity1 := newTestEntity("bulk1")
		entity2 := newTestEntity("bulk2")
		entityWithCustomID := struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}{
			ID:   "bulk-custom",
			Name: "bulk-entity",
		}

		bulker.Insert(entity1, entity2, entityWithCustomID)

		result, err := coll.BulkWrite(ctx, bulker.Models(), false)
		if err != nil {
			t.Error(err)
		}
		if result.InsertedCount != 3 {
			t.Errorf("expected 3 inserted documents, got %d", result.InsertedCount)
		}

		// Verify documents were inserted
		count, err := coll.Count(ctx, mongox.M{"$or": []mongox.M{
			{"id": mongox.M{mongox.In: []string{"bulk1", "bulk2"}}},
			{"_id": "bulk-custom"},
		}})
		if err != nil {
			t.Error(err)
		}
		if count != 3 {
			t.Errorf("expected 3 documents in bulk write, got %d", count)
		}

		// Cleanup
		_, _ = coll.DeleteMany(ctx, mongox.M{"$or": []mongox.M{
			{"id": mongox.M{mongox.In: []string{"bulk1", "bulk2"}}},
			{"_id": "bulk-custom"},
		}})
	})

	// Final cleanup
	_, err := coll.DeleteMany(ctx, nil)
	if err != nil && !errors.Is(err, mongox.ErrNotFound) {
		t.Error(err)
	}
}
