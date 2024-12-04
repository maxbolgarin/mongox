package mongox_test

import (
	"context"
	"errors"
	"log"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"math/rand"

	"github.com/maxbolgarin/lang"
	"github.com/maxbolgarin/mongox"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

// TODO: errors
// TODO: updates
// TODO: async

var client *mongox.Client

const (
	dbName = "mongox"

	findOneCollection = "find_one"
	findCollection    = "find"
	findAllCollection = "find_all"

	indexSingleCollection = "index_single"
	indexManyCollection   = "index_many"
	textCollection        = "text"
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
		err = db.Collection(indexSingleCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		err = db.Collection(indexSingleCollection).Insert(ctx, entity1)
		if err == nil {
			t.Error("expected error, got nil")
		}
		err = mongox.CreateIndex(ctx, db.Collection(indexSingleCollection), true, "id")
		if errors.Is(err, mongox.ErrIndexAlreadyExists) {
			t.Error(err)
		}
	})

	t.Run("IndexMany", func(t *testing.T) {
		err := db.Collection(indexManyCollection).CreateIndex(ctx, true, "id", "name", "number")
		if err != nil {
			t.Error(err)
		}
		entity1 := newTestEntity("1")
		err = db.Collection(indexManyCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		err = db.Collection(indexManyCollection).Insert(ctx, entity1)
		if err == nil {
			t.Error("expected error, got nil")
		}
		err = db.Collection(indexManyCollection).Insert(ctx, newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}
		err = db.Collection(indexManyCollection).Insert(ctx, newTestEntity("1"), newTestEntity("1"), newTestEntity("1"))
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Text", func(t *testing.T) {
		entity1 := newTestEntity("1")
		entity1.Name = "Running tool: /usr/local/go/bin/go test -timeout 45s -run ^TestFind$ github.com/maxbolgarin/mongox"
		err := db.Collection(textCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}

		entity2 := newTestEntity("2")
		entity2.Name = "Pairs in tool must be in the form NewF(key1, value1, key2, value2, ...)"
		err = db.Collection(textCollection).Insert(ctx, entity2)
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

		err = mongox.CreateTextIndex(ctx, db.Collection(textCollection), "en", "name")
		if errors.Is(err, mongox.ErrIndexAlreadyExists) {
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
			err := db.Collection(findOneCollection).Insert(ctx, entity1)
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		// Transaction is available only for replica sets or Sharded Clusters, not for standalone servers.
		if !errors.Is(err, mongox.ErrIllegalOperation) {
			t.Errorf("expected error %v, got %v", mongox.ErrIllegalOperation, err)
		}

		err = db.Collection(findOneCollection).Insert(ctx, entity1)
		if err != nil {
			t.Error(err)
		}
		entity2 := newTestEntity("2")
		err = mongox.Insert(ctx, db.Collection(findOneCollection), entity2)
		if err != nil {
			t.Error(err)
		}
		err = db.Collection(findOneCollection).Insert(ctx, newTestEntity("3"), newTestEntity("4"))
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
		if err := db.Collection(findOneCollection).Replace(ctx, entity1, mongox.M{"id": "1"}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "1"))
		testFindOne(t, ctx, db, entity1, mongox.M{"name": oldName}, mongox.ErrNotFound)
		testFindOne(t, ctx, db, entity1, mongox.M{"name": entity1.Name})

		oldNumber := entity1.Number
		entity1.Number = entity1.Number + 1
		if err := mongox.Replace(ctx, db.Collection(findOneCollection), entity1, mongox.M{"name": entity1.Name}); err != nil {
			t.Error(err)
		}
		testFindOne(t, ctx, db, entity1, mongox.NewM("id", "1"))
		testFindOne(t, ctx, db, entity1, mongox.M{"number": oldNumber}, mongox.ErrNotFound)
		testFindOne(t, ctx, db, entity1, mongox.M{"number": entity1.Number})

		testFindOne(t, ctx, db, entity2, mongox.NewM("id", "2"))
		if err := mongox.Upsert(ctx, db.Collection(findOneCollection), entity1, mongox.M{"id": "2"}); err != nil {
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
		err := db.Collection(findCollection).Insert(ctx, entity2, entity3, entity4)
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

		if err := db.Collection(findCollection).DeleteMany(ctx, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}}); err != nil {
			t.Error(err)
		}
		testFind(t, ctx, db, []testEntity{}, mongox.M{"id": mongox.M{mongox.In: []string{"2", "3"}}})

		if err := mongox.DeleteMany(ctx, db.Collection(findCollection), nil); err != nil {
			t.Error(err)
		}
		testFind(t, ctx, db, []testEntity{}, nil)

		err = mongox.DeleteMany(ctx, db.Collection(findCollection), nil)
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
			err = db.Collection(findAllCollection).Insert(ctx, newTestEntity(strconv.Itoa(i)), newTestEntity(strconv.Itoa(i)),
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
	var result testEntity
	err := db.Collection(findOneCollection).FindOne(ctx, &result, filter)
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
		Number: rand.Intn(26),
		Bool:   true,
		Slice:  []int{rand.Intn(26), rand.Intn(26), rand.Intn(26)},
		Array:  [3]int{rand.Intn(26), rand.Intn(26), rand.Intn(26)},
		Map:    map[string]int{"1": rand.Intn(26), randString(): rand.Intn(26), randString(): rand.Intn(26)},
		Time:   time.Date(2000, 1, 1, 1, 2, 3, 0, time.UTC),
		Struct: innerStruct{Name: randString(), Number: rand.Intn(26)},
	}
}
