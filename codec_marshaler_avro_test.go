package avro_test

import (
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test struct with value receiver methods
type CustomRecord struct {
	Name  string
	Value int
}

func (c CustomRecord) MarshalAvro(w *avro.Writer) error {
	// Custom encoding: write name as string, then value as int
	w.WriteString(c.Name)
	w.WriteInt(int32(c.Value))
	return nil
}

func (c *CustomRecord) UnmarshalAvro(r *avro.Reader) error {
	// Custom decoding: read name as string, then value as int
	c.Name = r.ReadString()
	c.Value = int(r.ReadInt())
	return nil
}

// Test struct with pointer receiver for both methods
type CustomRecordPtr struct {
	Data  string
	Count int
}

func (c *CustomRecordPtr) MarshalAvro(w *avro.Writer) error {
	w.WriteString(c.Data)
	w.WriteInt(int32(c.Count))
	return nil
}

func (c *CustomRecordPtr) UnmarshalAvro(r *avro.Reader) error {
	c.Data = r.ReadString()
	c.Count = int(r.ReadInt())
	return nil
}

// Test struct that returns an error during marshaling
type ErrorMarshaler struct {
	Value int
}

func (e ErrorMarshaler) MarshalAvro(w *avro.Writer) error {
	if e.Value < 0 {
		return errors.New("marshaling error")
	}
	w.WriteInt(int32(e.Value))
	return nil
}

func (e *ErrorMarshaler) UnmarshalAvro(r *avro.Reader) error {
	e.Value = int(r.ReadInt())
	if e.Value > 100 {
		return errors.New("unmarshaling error")
	}
	return nil
}

// Test struct with nested custom marshaler
type Container struct {
	ID     int
	Record CustomRecord
}

func TestAvroMarshaler_ValueReceiver(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecord",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`

	record := CustomRecord{
		Name:  "test",
		Value: 42,
	}

	// Marshal
	data, err := avro.Marshal(avro.MustParse(schema), record)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal
	var result CustomRecord
	err = avro.Unmarshal(avro.MustParse(schema), data, &result)
	require.NoError(t, err)
	assert.Equal(t, "test", result.Name)
	assert.Equal(t, 42, result.Value)
}

func TestAvroMarshaler_PointerReceiver(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecordPtr",
		"fields": [
			{"name": "data", "type": "string"},
			{"name": "count", "type": "int"}
		]
	}`

	record := &CustomRecordPtr{
		Data:  "hello",
		Count: 100,
	}

	// Marshal
	data, err := avro.Marshal(avro.MustParse(schema), record)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal
	var result CustomRecordPtr
	err = avro.Unmarshal(avro.MustParse(schema), data, &result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result.Data)
	assert.Equal(t, 100, result.Count)
}

func TestAvroMarshaler_ErrorHandling(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "ErrorMarshaler",
		"fields": [
			{"name": "value", "type": "int"}
		]
	}`

	t.Run("marshal error", func(t *testing.T) {
		record := ErrorMarshaler{Value: -1}
		_, err := avro.Marshal(avro.MustParse(schema), record)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "marshaling error")
	})

	t.Run("unmarshal error", func(t *testing.T) {
		// Create data with value > 100 that will trigger unmarshal error
		errorRecord := ErrorMarshaler{Value: 200}
		errorData, err := avro.Marshal(avro.MustParse(schema), errorRecord)
		require.NoError(t, err)

		var result ErrorMarshaler
		err = avro.Unmarshal(avro.MustParse(schema), errorData, &result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshaling error")
	})
}

func TestAvroMarshaler_NilPointer(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecordPtr",
		"fields": [
			{"name": "data", "type": "string"},
			{"name": "count", "type": "int"}
		]
	}`

	// Prepare some data
	record := &CustomRecordPtr{Data: "test", Count: 5}
	data, err := avro.Marshal(avro.MustParse(schema), record)
	require.NoError(t, err)

	// Unmarshal into a nil pointer (should allocate)
	var result *CustomRecordPtr
	err = avro.Unmarshal(avro.MustParse(schema), data, &result)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "test", result.Data)
	assert.Equal(t, 5, result.Count)
}

func TestAvroMarshaler_RoundTrip(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecord",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`

	original := CustomRecord{
		Name:  "round-trip-test",
		Value: 999,
	}

	// Marshal
	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	// Unmarshal
	var decoded CustomRecord
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Value, decoded.Value)

	// Re-marshal and verify same bytes
	data2, err := avro.Marshal(avro.MustParse(schema), decoded)
	require.NoError(t, err)
	assert.Equal(t, data, data2)
}

func TestAvroMarshaler_OnlyWorksWithRecords(t *testing.T) {
	// Test that AvroMarshaler is only used for record schemas, not primitives

	// Define a custom type that implements AvroMarshaler
	type CustomString string

	// This won't use MarshalAvro because it's not a record schema
	schema := `"string"`
	value := CustomString("test")

	// Should work but won't use custom marshaler
	data, err := avro.Marshal(avro.MustParse(schema), value)
	require.NoError(t, err)
	assert.NotEmpty(t, data)
}

func TestAvroMarshaler_WithEncoder(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecord",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`

	records := []CustomRecord{
		{Name: "first", Value: 1},
		{Name: "second", Value: 2},
		{Name: "third", Value: 3},
	}

	// Test with encoder
	var buf []byte
	encoder := avro.NewEncoderForSchema(avro.MustParse(schema), &testWriter{buf: &buf})

	for _, rec := range records {
		err := encoder.Encode(rec)
		require.NoError(t, err)
	}

	// Decode all records
	decoder := avro.NewDecoderForSchema(avro.MustParse(schema), &testReader{buf: buf})

	for _, rec := range records {
		var decoded CustomRecord
		err := decoder.Decode(&decoded)
		require.NoError(t, err)
		assert.Equal(t, rec.Name, decoded.Name)
		assert.Equal(t, rec.Value, decoded.Value)
	}
}

func TestAvroMarshaler_ComplexData(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomRecord",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`

	testCases := []struct {
		name   string
		record CustomRecord
	}{
		{
			name:   "empty string",
			record: CustomRecord{Name: "", Value: 0},
		},
		{
			name:   "long string",
			record: CustomRecord{Name: string(make([]byte, 1000)), Value: -1},
		},
		{
			name:   "negative value",
			record: CustomRecord{Name: "negative", Value: -12345},
		},
		{
			name:   "max int",
			record: CustomRecord{Name: "max", Value: 2147483647},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := avro.Marshal(avro.MustParse(schema), tc.record)
			require.NoError(t, err)

			var decoded CustomRecord
			err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
			require.NoError(t, err)
			assert.Equal(t, tc.record.Name, decoded.Name)
			assert.Equal(t, tc.record.Value, decoded.Value)
		})
	}
}

// Helper types for encoder/decoder test
type testWriter struct {
	buf *[]byte
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

type testReader struct {
	buf []byte
	pos int
}

func (r *testReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.buf) {
		return 0, nil
	}
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

// StandardPerson uses standard avro struct tags and serialization
type StandardPerson struct {
	Name string `avro:"name"`
	Age  int32  `avro:"age"`
	City string `avro:"city"`
}

// CustomPerson uses custom marshaling but produces the same wire format
type CustomPerson struct {
	Name string
	Age  int32
	City string
}

func (p CustomPerson) MarshalAvro(w *avro.Writer) error {
	// Marshal in the same order as StandardPerson fields in schema
	w.WriteString(p.Name)
	w.WriteInt(p.Age)
	w.WriteString(p.City)
	return nil
}

func (p *CustomPerson) UnmarshalAvro(r *avro.Reader) error {
	// Unmarshal in the same order as StandardPerson fields in schema
	p.Name = r.ReadString()
	p.Age = r.ReadInt()
	p.City = r.ReadString()
	return nil
}

// TestWireCompatibility verifies that custom marshaling produces the same
// wire format as standard avro marshaling, allowing interoperability.
func TestWireCompatibility(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "city", "type": "string"}
		]
	}`

	t.Run("standard to custom", func(t *testing.T) {
		// Encode with standard struct
		standard := StandardPerson{
			Name: "Alice",
			Age:  30,
			City: "San Francisco",
		}

		data, err := avro.Marshal(avro.MustParse(schema), standard)
		require.NoError(t, err)

		// Decode with custom struct
		var custom CustomPerson
		err = avro.Unmarshal(avro.MustParse(schema), data, &custom)
		require.NoError(t, err)

		assert.Equal(t, standard.Name, custom.Name)
		assert.Equal(t, standard.Age, custom.Age)
		assert.Equal(t, standard.City, custom.City)
	})

	t.Run("custom to standard", func(t *testing.T) {
		// Encode with custom struct
		custom := CustomPerson{
			Name: "Bob",
			Age:  25,
			City: "New York",
		}

		data, err := avro.Marshal(avro.MustParse(schema), custom)
		require.NoError(t, err)

		// Decode with standard struct
		var standard StandardPerson
		err = avro.Unmarshal(avro.MustParse(schema), data, &standard)
		require.NoError(t, err)

		assert.Equal(t, custom.Name, standard.Name)
		assert.Equal(t, custom.Age, standard.Age)
		assert.Equal(t, custom.City, standard.City)
	})

	t.Run("same bytes produced", func(t *testing.T) {
		// Create identical data
		standard := StandardPerson{
			Name: "Charlie",
			Age:  35,
			City: "Seattle",
		}

		custom := CustomPerson{
			Name: "Charlie",
			Age:  35,
			City: "Seattle",
		}

		// Marshal both
		standardData, err := avro.Marshal(avro.MustParse(schema), standard)
		require.NoError(t, err)

		customData, err := avro.Marshal(avro.MustParse(schema), custom)
		require.NoError(t, err)

		// Should produce identical bytes
		assert.Equal(t, standardData, customData, "standard and custom marshaling should produce identical bytes")
	})

	t.Run("round trip both ways", func(t *testing.T) {
		// Start with standard
		original := StandardPerson{
			Name: "Diana",
			Age:  40,
			City: "Boston",
		}

		// standard -> bytes
		data1, err := avro.Marshal(avro.MustParse(schema), original)
		require.NoError(t, err)

		// bytes -> custom
		var custom CustomPerson
		err = avro.Unmarshal(avro.MustParse(schema), data1, &custom)
		require.NoError(t, err)

		// custom -> bytes
		data2, err := avro.Marshal(avro.MustParse(schema), custom)
		require.NoError(t, err)

		// bytes -> standard
		var result StandardPerson
		err = avro.Unmarshal(avro.MustParse(schema), data2, &result)
		require.NoError(t, err)

		// Verify round trip
		assert.Equal(t, original, result)
		assert.Equal(t, data1, data2, "bytes should remain identical through round trip")
	})
}

// Address is a nested struct with custom marshaling
type Address struct {
	Street  string
	City    string
	ZipCode string
}

func (a Address) MarshalAvro(w *avro.Writer) error {
	w.WriteString(a.Street)
	w.WriteString(a.City)
	w.WriteString(a.ZipCode)
	return nil
}

func (a *Address) UnmarshalAvro(r *avro.Reader) error {
	a.Street = r.ReadString()
	a.City = r.ReadString()
	a.ZipCode = r.ReadString()
	return nil
}

// Employee contains a nested struct with custom marshaling
type Employee struct {
	ID      int32
	Name    string
	Address Address
}

func (e Employee) MarshalAvro(w *avro.Writer) error {
	w.WriteInt(e.ID)
	w.WriteString(e.Name)
	// Nested struct - marshal its fields
	return e.Address.MarshalAvro(w)
}

func (e *Employee) UnmarshalAvro(r *avro.Reader) error {
	e.ID = r.ReadInt()
	e.Name = r.ReadString()
	// Nested struct - unmarshal its fields
	return e.Address.UnmarshalAvro(r)
}

// TestNestedCustomMarshaling tests custom marshaling with nested structs
func TestNestedCustomMarshaling(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Employee",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{
				"name": "address",
				"type": {
					"type": "record",
					"name": "Address",
					"fields": [
						{"name": "street", "type": "string"},
						{"name": "city", "type": "string"},
						{"name": "zipCode", "type": "string"}
					]
				}
			}
		]
	}`

	employee := Employee{
		ID:   12345,
		Name: "John Smith",
		Address: Address{
			Street:  "123 Main St",
			City:    "Springfield",
			ZipCode: "12345",
		},
	}

	// Marshal
	data, err := avro.Marshal(avro.MustParse(schema), employee)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal
	var decoded Employee
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	// Verify all fields including nested
	assert.Equal(t, employee.ID, decoded.ID)
	assert.Equal(t, employee.Name, decoded.Name)
	assert.Equal(t, employee.Address.Street, decoded.Address.Street)
	assert.Equal(t, employee.Address.City, decoded.Address.City)
	assert.Equal(t, employee.Address.ZipCode, decoded.Address.ZipCode)
}

// Account with nullable fields
type Account struct {
	ID       int32
	Username string
	Email    *string // nullable
	Phone    *string // nullable
}

func (a Account) MarshalAvro(w *avro.Writer) error {
	w.WriteInt(a.ID)
	w.WriteString(a.Username)

	// Handle nullable email (union with null)
	if a.Email == nil {
		w.WriteLong(0) // null type index in union
	} else {
		w.WriteLong(1) // string type index in union
		w.WriteString(*a.Email)
	}

	// Handle nullable phone (union with null)
	if a.Phone == nil {
		w.WriteLong(0) // null type index in union
	} else {
		w.WriteLong(1) // string type index in union
		w.WriteString(*a.Phone)
	}

	return nil
}

func (a *Account) UnmarshalAvro(r *avro.Reader) error {
	a.ID = r.ReadInt()
	a.Username = r.ReadString()

	// Handle nullable email
	emailIndex := r.ReadLong()
	if emailIndex == 0 {
		a.Email = nil
	} else {
		email := r.ReadString()
		a.Email = &email
	}

	// Handle nullable phone
	phoneIndex := r.ReadLong()
	if phoneIndex == 0 {
		a.Phone = nil
	} else {
		phone := r.ReadString()
		a.Phone = &phone
	}

	return nil
}

// TestNullableFieldsCustomMarshaling tests custom marshaling with nullable fields
func TestNullableFieldsCustomMarshaling(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Account",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "username", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null},
			{"name": "phone", "type": ["null", "string"], "default": null}
		]
	}`

	t.Run("all fields populated", func(t *testing.T) {
		email := "user@example.com"
		phone := "555-1234"

		account := Account{
			ID:       1001,
			Username: "johndoe",
			Email:    &email,
			Phone:    &phone,
		}

		// Marshal
		data, err := avro.Marshal(avro.MustParse(schema), account)
		require.NoError(t, err)

		// Unmarshal
		var decoded Account
		err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, account.ID, decoded.ID)
		assert.Equal(t, account.Username, decoded.Username)
		require.NotNil(t, decoded.Email)
		assert.Equal(t, *account.Email, *decoded.Email)
		require.NotNil(t, decoded.Phone)
		assert.Equal(t, *account.Phone, *decoded.Phone)
	})

	t.Run("nullable fields are null", func(t *testing.T) {
		account := Account{
			ID:       1002,
			Username: "janedoe",
			Email:    nil,
			Phone:    nil,
		}

		// Marshal
		data, err := avro.Marshal(avro.MustParse(schema), account)
		require.NoError(t, err)

		// Unmarshal
		var decoded Account
		err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, account.ID, decoded.ID)
		assert.Equal(t, account.Username, decoded.Username)
		assert.Nil(t, decoded.Email)
		assert.Nil(t, decoded.Phone)
	})

	t.Run("mixed nullable fields", func(t *testing.T) {
		email := "admin@example.com"

		account := Account{
			ID:       1003,
			Username: "admin",
			Email:    &email,
			Phone:    nil, // only email is set
		}

		// Marshal
		data, err := avro.Marshal(avro.MustParse(schema), account)
		require.NoError(t, err)

		// Unmarshal
		var decoded Account
		err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, account.ID, decoded.ID)
		assert.Equal(t, account.Username, decoded.Username)
		require.NotNil(t, decoded.Email)
		assert.Equal(t, *account.Email, *decoded.Email)
		assert.Nil(t, decoded.Phone)
	})
}
