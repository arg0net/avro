package avro_test

import (
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
)

// User demonstrates a struct with custom Avro marshaling.
// This example shows how to implement custom serialization logic
// while still integrating with the Avro library.
type User struct {
	ID        int
	Name      string
	CreatedAt time.Time
}

// MarshalAvro implements custom marshaling for User.
// The method uses the public Writer API to serialize fields.
func (u User) MarshalAvro(w *avro.Writer) error {
	// Serialize ID and Name
	w.WriteInt(int32(u.ID))
	w.WriteString(u.Name)

	// Serialize CreatedAt as Unix timestamp (long)
	w.WriteLong(u.CreatedAt.Unix())

	return nil
}

// UnmarshalAvro implements custom unmarshaling for User.
// The method uses the public Reader API to deserialize fields.
func (u *User) UnmarshalAvro(r *avro.Reader) error {
	// Deserialize ID and Name
	u.ID = int(r.ReadInt())
	u.Name = r.ReadString()

	// Deserialize CreatedAt from Unix timestamp
	timestamp := r.ReadLong()
	u.CreatedAt = time.Unix(timestamp, 0).UTC()

	return nil
}

// Example_customMarshaling demonstrates how to use custom Avro marshaling
// with structs that implement custom marshaling interfaces.
func Example_customMarshaling() {
	// Define the schema
	schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "created_at", "type": "long"}
		]
	}`

	// Create a user with a specific timestamp
	user := User{
		ID:        42,
		Name:      "John Doe",
		CreatedAt: time.Unix(1609459200, 0), // 2021-01-01 00:00:00 UTC
	}

	// Marshal the user
	data, err := avro.Marshal(avro.MustParse(schema), user)
	if err != nil {
		panic(err)
	}

	// Unmarshal the user
	var decoded User
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	if err != nil {
		panic(err)
	}

	fmt.Printf("ID: %d\n", decoded.ID)
	fmt.Printf("Name: %s\n", decoded.Name)
	fmt.Printf("Created: %s\n", decoded.CreatedAt.Format("2006-01-02"))

	// Output:
	// ID: 42
	// Name: John Doe
	// Created: 2021-01-01
}
