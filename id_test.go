package mongox

import (
	"bytes"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestIDMarshalBSONValue verifies that ID encodes as a real BSON ObjectID,
// not as a generic [12]byte array/binary (regression test).
func TestIDMarshalBSONValue(t *testing.T) {
	oid := bson.NewObjectID()
	id := NewIDFromObjectID(oid)

	gotType, gotData, err := bson.MarshalValue(id)
	if err != nil {
		t.Fatalf("MarshalValue(ID): %v", err)
	}
	wantType, wantData, err := bson.MarshalValue(oid)
	if err != nil {
		t.Fatalf("MarshalValue(ObjectID): %v", err)
	}
	if gotType != wantType {
		t.Fatalf("ID marshaled as BSON type %v, want %v (ObjectID)", gotType, wantType)
	}
	if !bytes.Equal(gotData, wantData) {
		t.Fatalf("ID marshaled bytes %v, want %v", gotData, wantData)
	}
}

// TestIDRoundTrip verifies that documents written with ID fields can be read
// back into bson.ObjectID fields and vice versa.
func TestIDRoundTrip(t *testing.T) {
	oid := bson.NewObjectID()

	rawFromID, err := bson.Marshal(struct {
		ID ID `bson:"_id"`
	}{ID: NewIDFromObjectID(oid)})
	if err != nil {
		t.Fatalf("marshal struct with ID: %v", err)
	}

	var asObjectID struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := bson.Unmarshal(rawFromID, &asObjectID); err != nil {
		t.Fatalf("unmarshal into ObjectID: %v", err)
	}
	if asObjectID.ID != oid {
		t.Fatalf("got ObjectID %v, want %v", asObjectID.ID, oid)
	}

	var asID struct {
		ID ID `bson:"_id"`
	}
	if err := bson.Unmarshal(rawFromID, &asID); err != nil {
		t.Fatalf("unmarshal into ID: %v", err)
	}
	if asID.ID.ObjectID() != oid {
		t.Fatalf("got ID %v, want %v", asID.ID, oid)
	}
}

func TestIDMarshalJSON(t *testing.T) {
	oid := bson.NewObjectID()
	id := NewIDFromObjectID(oid)

	data, err := id.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	want := `"` + oid.Hex() + `"`
	if string(data) != want {
		t.Fatalf("MarshalJSON = %s, want %s", data, want)
	}

	var back ID
	if err := back.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if back != id {
		t.Fatalf("round trip = %v, want %v", back, id)
	}
}

func TestIDIsZero(t *testing.T) {
	var zero ID
	if !zero.IsZero() {
		t.Fatal("zero ID must report IsZero() == true")
	}
	if NewIDFromObjectID(bson.NewObjectID()).IsZero() {
		t.Fatal("generated ID must report IsZero() == false")
	}
}
