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

		result, err = mongox.FindAll[testEntity](ctx, db.Collection(findAllCollection), mongox.FindOptions{Sort: mongox.M{"id": 1}})
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
		_, err = coll.Insert(ctx, newEntity)
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity, mongox.M{"name": newEntity.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"number": newEntity.Number})
		testUpdate(t, ctx, db, newEntity, mongox.M{"struct.name": newEntity.Struct.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"time": newEntity.Time})

		updTestEntity := struct {
			Name   *string    `bson:"name"`
			Number *int       `bson:"number"`
			Time   *time.Time `bson:"time"`
			Struct *struct {
				Name *string `bson:"name"`
			} `bson:"struct"`
		}{
			Name:   lang.Ptr("new-name"),
			Number: lang.Ptr(9999999),
			Time:   lang.Ptr(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
			Struct: &struct {
				Name *string `bson:"name"`
			}{
				Name: lang.Ptr("new-struct-name"),
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

		newEntity, err = mongox.FindOne[testEntity](ctx, coll, mongox.M{"id": "1"})
		if err != nil {
			t.Error(err)
		}

		testUpdate(t, ctx, db, newEntity, mongox.M{"name": newEntity.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"number": newEntity.Number})
		testUpdate(t, ctx, db, newEntity, mongox.M{"struct.name": newEntity.Struct.Name})
		testUpdate(t, ctx, db, newEntity, mongox.M{"time": newEntity.Time})
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
		if !errors.Is(err, mongox.ErrInvalidArgument) {
			t.Errorf("expected error %v, got %v", mongox.ErrInvalidArgument, err)
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
		if err != nil {
			t.Error(err)
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
		if _, err = coll.Upsert(ctx, entity, f); !errors.Is(err, mongox.ErrNotFound) {
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
	ID     string         `bson:"id"`
	Name   string         `bson:"name"`
	Number int            `bson:"number"`
	Bool   bool           `bson:"bool"`
	Slice  []int          `bson:"slice"`
	Array  [3]int         `bson:"array"`
	Map    map[string]int `bson:"map"`
	Time   time.Time      `bson:"time"`
	Struct innerStruct    `bson:"struct"`
}

type innerStruct struct {
	Name   string `bson:"name"`
	Number int    `bson:"number"`
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
