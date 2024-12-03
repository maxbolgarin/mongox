package mongox

import (
	"errors"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// M is a map containing query operators to filter documents.
type M map[string]any

// NewM creates a new Filter based on pairs.
// Pairs must be in the form NewF(key1, value1, key2, value2, ...)
func NewM(pairs ...any) M {
	return newMapFromPairs(pairs...)
}

// Add adds pairs to the Filter.
func (f M) Add(pairs ...any) M {
	addPairs(f, pairs...)
	return f
}

// Prepare returns a bson.D representation of the Filter that can be used in a MongoDB query.
func (f M) Prepare() bson.D {
	filter := make(bson.D, 0, len(f))
	for k, v := range f {
		filter = append(filter, bson.E{Key: k, Value: v})
	}
	return filter
}

// String returns a string representation of the Filter.
func (f M) String() string {
	return f.Prepare().String()
}

func newMapFromPairs(pairs ...any) map[string]any {
	out := make(map[string]any, len(pairs)/2)
	addPairs(out, pairs...)
	return out
}

func addPairs(m map[string]any, pairs ...any) {
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if ok && i+1 < len(pairs) {
			m[key] = pairs[i+1]
		}
	}
}

func prepareUpdates(upd map[string]any, op string) bson.D {
	res := make(bson.D, 0, len(upd))
	for k, v := range upd {
		res = append(res, bson.E{Key: k, Value: v})
	}
	return bson.D{bson.E{Key: op, Value: res}}
}

func diffToUpdates(diff any) (bson.D, error) {
	upd, err := processDiffStruct(diff, "")
	if err != nil {
		return nil, err
	}
	return prepareUpdates(upd, Set), nil
}

func processDiffStruct(diff any, parentField string) (map[string]any, error) {
	req := reflect.ValueOf(diff)
	if req.Kind() != reflect.Struct {
		return nil, errors.New("only struct fields are allowed")
	}

	upd := make(map[string]any)
	for n := 0; n < req.NumField(); n++ {
		fieldName := req.Type().Field(n).Tag.Get("bson")
		if parentField != "" {
			fieldName = parentField + "." + fieldName
		}

		field := req.Field(n)
		if !field.CanInterface() {
			// unexported field
			continue
		}

		kind := field.Kind()
		if kind != reflect.Ptr && kind != reflect.Array && kind != reflect.Slice && kind != reflect.Map {
			// expect pointers or slice/
			continue
		}

		if field.IsNil() {
			// nil == no update for field
			continue
		}

		if kind == reflect.Pointer {
			// get value of pointer
			field = field.Elem()
		}

		if field.Kind() == reflect.Struct {
			i := field.Interface()
			if _, ok := i.(time.Time); ok {
				upd[fieldName] = i
				continue
			}
			if _, ok := i.(time.Duration); ok {
				upd[fieldName] = i
				continue
			}

			structUpd, err := processDiffStruct(field.Interface(), fieldName)
			if err != nil {
				continue
			}
			for k, v := range structUpd {
				upd[k] = v
			}
			continue
		}

		upd[fieldName] = field.Interface()
	}

	if len(upd) == 0 {
		return nil, errors.New("updates are empty")
	}

	return upd, nil
}
