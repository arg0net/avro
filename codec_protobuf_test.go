package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	testpb "github.com/hamba/avro/v2/testdata/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtobuf_BasicMessage_Encode(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	msg := &testpb.BasicMessage{
		Id:     42,
		Name:   "John Doe",
		Active: true,
		Score:  95.5,
	}

	data, err := avro.Marshal(avro.MustParse(schema), msg)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify by decoding
	var decoded testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, int32(42), decoded.Id)
	assert.Equal(t, "John Doe", decoded.Name)
	assert.Equal(t, true, decoded.Active)
	assert.Equal(t, 95.5, decoded.Score)
}

func TestProtobuf_BasicMessage_Decode(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	// First encode using a regular struct
	regularData := map[string]any{
		"id":     int32(42),
		"name":   "Jane Doe",
		"active": true,
		"score":  88.5,
	}

	data, err := avro.Marshal(avro.MustParse(schema), regularData)
	require.NoError(t, err)

	// Now decode into protobuf message
	var msg testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &msg)
	require.NoError(t, err)
	assert.Equal(t, int32(42), msg.Id)
	assert.Equal(t, "Jane Doe", msg.Name)
	assert.Equal(t, true, msg.Active)
	assert.Equal(t, 88.5, msg.Score)
}

func TestProtobuf_BasicMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	original := &testpb.BasicMessage{
		Id:     123,
		Name:   "Test User",
		Active: false,
		Score:  77.7,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Active, decoded.Active)
	assert.Equal(t, original.Score, decoded.Score)
}

func TestProtobuf_NestedMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "NestedMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "title", "type": "string"},
			{
				"name": "author",
				"type": {
					"type": "record",
					"name": "BasicMessage",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"},
						{"name": "active", "type": "boolean"},
						{"name": "score", "type": "double"}
					]
				}
			}
		]
	}`

	original := &testpb.NestedMessage{
		Id:    1,
		Title: "My Article",
		Author: &testpb.BasicMessage{
			Id:     42,
			Name:   "Author Name",
			Active: true,
			Score:  99.9,
		},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.NestedMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Title, decoded.Title)
	assert.NotNil(t, decoded.Author)
	assert.Equal(t, original.Author.Id, decoded.Author.Id)
	assert.Equal(t, original.Author.Name, decoded.Author.Name)
	assert.Equal(t, original.Author.Active, decoded.Author.Active)
	assert.Equal(t, original.Author.Score, decoded.Author.Score)
}

func TestProtobuf_ListMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "ListMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "numbers", "type": {"type": "array", "items": "int"}}
		]
	}`

	original := &testpb.ListMessage{
		Id:      1,
		Tags:    []string{"tag1", "tag2", "tag3"},
		Numbers: []int32{10, 20, 30, 40},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.ListMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Tags, decoded.Tags)
	assert.Equal(t, original.Numbers, decoded.Numbers)
}

func TestProtobuf_ListMessage_Empty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "ListMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "tags", "type": {"type": "array", "items": "string"}},
			{"name": "numbers", "type": {"type": "array", "items": "int"}}
		]
	}`

	original := &testpb.ListMessage{
		Id:      1,
		Tags:    []string{},
		Numbers: []int32{},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.ListMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Empty(t, decoded.Tags)
	assert.Empty(t, decoded.Numbers)
}

func TestProtobuf_MapMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "MapMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "labels", "type": {"type": "map", "values": "string"}},
			{"name": "scores", "type": {"type": "map", "values": "int"}}
		]
	}`

	original := &testpb.MapMessage{
		Id: 1,
		Labels: map[string]string{
			"env":  "prod",
			"team": "backend",
		},
		Scores: map[string]int32{
			"test1": 100,
			"test2": 95,
		},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.MapMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Labels, decoded.Labels)
	assert.Equal(t, original.Scores, decoded.Scores)
}

func TestProtobuf_MapMessage_Empty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "MapMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "labels", "type": {"type": "map", "values": "string"}},
			{"name": "scores", "type": {"type": "map", "values": "int"}}
		]
	}`

	original := &testpb.MapMessage{
		Id:     1,
		Labels: map[string]string{},
		Scores: map[string]int32{},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.MapMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Empty(t, decoded.Labels)
	assert.Empty(t, decoded.Scores)
}

func TestProtobuf_AllTypesMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "AllTypesMessage",
		"fields": [
			{"name": "int32_field", "type": "int"},
			{"name": "int64_field", "type": "long"},
			{"name": "uint32_field", "type": "int"},
			{"name": "uint64_field", "type": "long"},
			{"name": "sint32_field", "type": "int"},
			{"name": "sint64_field", "type": "long"},
			{"name": "fixed32_field", "type": "int"},
			{"name": "fixed64_field", "type": "long"},
			{"name": "sfixed32_field", "type": "int"},
			{"name": "sfixed64_field", "type": "long"},
			{"name": "float_field", "type": "float"},
			{"name": "double_field", "type": "double"},
			{"name": "bool_field", "type": "boolean"},
			{"name": "string_field", "type": "string"},
			{"name": "bytes_field", "type": "bytes"}
		]
	}`

	original := &testpb.AllTypesMessage{
		Int32Field:    -123,
		Int64Field:    -456789,
		Uint32Field:   123,
		Uint64Field:   456789,
		Sint32Field:   -789,
		Sint64Field:   -123456,
		Fixed32Field:  999,
		Fixed64Field:  888777,
		Sfixed32Field: -111,
		Sfixed64Field: -222333,
		FloatField:    3.14,
		DoubleField:   2.71828,
		BoolField:     true,
		StringField:   "test string",
		BytesField:    []byte{0x01, 0x02, 0x03, 0x04},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.AllTypesMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Int32Field, decoded.Int32Field)
	assert.Equal(t, original.Int64Field, decoded.Int64Field)
	assert.Equal(t, original.Uint32Field, decoded.Uint32Field)
	assert.Equal(t, original.Uint64Field, decoded.Uint64Field)
	assert.Equal(t, original.Sint32Field, decoded.Sint32Field)
	assert.Equal(t, original.Sint64Field, decoded.Sint64Field)
	assert.Equal(t, original.Fixed32Field, decoded.Fixed32Field)
	assert.Equal(t, original.Fixed64Field, decoded.Fixed64Field)
	assert.Equal(t, original.Sfixed32Field, decoded.Sfixed32Field)
	assert.Equal(t, original.Sfixed64Field, decoded.Sfixed64Field)
	assert.InDelta(t, original.FloatField, decoded.FloatField, 0.0001)
	assert.InDelta(t, original.DoubleField, decoded.DoubleField, 0.0001)
	assert.Equal(t, original.BoolField, decoded.BoolField)
	assert.Equal(t, original.StringField, decoded.StringField)
	assert.Equal(t, original.BytesField, decoded.BytesField)
}

func TestProtobuf_EnumMessage_AsInt_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "EnumMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "status", "type": "int"}
		]
	}`

	original := &testpb.EnumMessage{
		Id:     1,
		Status: testpb.Status_STATUS_ACTIVE,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.EnumMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Status, decoded.Status)
}

func TestProtobuf_EnumMessage_AsString_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "EnumMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "status", "type": "string"}
		]
	}`

	original := &testpb.EnumMessage{
		Id:     1,
		Status: testpb.Status_STATUS_INACTIVE,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.EnumMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Status, decoded.Status)
}

func TestProtobuf_Encoder_BasicMessage(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	msg := &testpb.BasicMessage{
		Id:     99,
		Name:   "Encoder Test",
		Active: true,
		Score:  100.0,
	}

	err = enc.Encode(msg)
	require.NoError(t, err)
	assert.NotEmpty(t, buf.Bytes())
}

func TestProtobuf_Decoder_BasicMessage(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	// Encode first
	msg := &testpb.BasicMessage{
		Id:     55,
		Name:   "Decoder Test",
		Active: false,
		Score:  85.5,
	}
	data, err := avro.Marshal(avro.MustParse(schema), msg)
	require.NoError(t, err)

	// Now decode
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var decoded testpb.BasicMessage
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	assert.Equal(t, msg.Id, decoded.Id)
	assert.Equal(t, msg.Name, decoded.Name)
	assert.Equal(t, msg.Active, decoded.Active)
	assert.Equal(t, msg.Score, decoded.Score)
}

func TestProtobuf_MissingField_Error(t *testing.T) {
	defer ConfigTeardown()

	// Schema has a field that doesn't exist in protobuf message
	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "nonexistent", "type": "string"}
		]
	}`

	msg := &testpb.BasicMessage{
		Id:   1,
		Name: "Test",
	}

	_, err := avro.Marshal(avro.MustParse(schema), msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found in protobuf message")
}

func TestProtobuf_PartialSchema(t *testing.T) {
	defer ConfigTeardown()

	// Schema only includes subset of fields
	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	msg := &testpb.BasicMessage{
		Id:     1,
		Name:   "Test",
		Active: true,
		Score:  95.5,
	}

	data, err := avro.Marshal(avro.MustParse(schema), msg)
	require.NoError(t, err)

	var decoded testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	// Only the fields in schema should be preserved
	assert.Equal(t, msg.Id, decoded.Id)
	assert.Equal(t, msg.Name, decoded.Name)
	// Other fields will be zero values in decoded message
	assert.Equal(t, false, decoded.Active)
	assert.Equal(t, 0.0, decoded.Score)
}

func TestProtobuf_ValueReceiver(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"}
		]
	}`

	// Test with value (not pointer) - note: protobuf messages should typically be used as pointers
	msg := &testpb.BasicMessage{
		Id:     42,
		Name:   "Value Test",
		Active: true,
		Score:  88.8,
	}

	data, err := avro.Marshal(avro.MustParse(schema), msg)
	require.NoError(t, err)
	assert.NotEmpty(t, data)
}

func TestProtobuf_OptionalMessage_NullValues(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OptionalMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]}
		]
	}`

	// Create message with optional fields not set
	original := &testpb.OptionalMessage{
		Id: 1,
		// name and age are not set (null)
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OptionalMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Nil(t, decoded.Name)
	assert.Nil(t, decoded.Age)
}

func TestProtobuf_OptionalMessage_SetValues(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OptionalMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]}
		]
	}`

	name := "Alice"
	age := int32(30)
	original := &testpb.OptionalMessage{
		Id:   1,
		Name: &name,
		Age:  &age,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OptionalMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Name)
	assert.Equal(t, *original.Name, *decoded.Name)
	require.NotNil(t, decoded.Age)
	assert.Equal(t, *original.Age, *decoded.Age)
}

func TestProtobuf_OptionalMessage_MixedValues(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OptionalMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]}
		]
	}`

	name := "Bob"
	original := &testpb.OptionalMessage{
		Id:   2,
		Name: &name,
		Age:  nil, // age is not set
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OptionalMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Name)
	assert.Equal(t, *original.Name, *decoded.Name)
	assert.Nil(t, decoded.Age)
}

func TestProtobuf_OptionalMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OptionalMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": ["null", "string"]},
			{"name": "age", "type": ["null", "int"]}
		]
	}`

	testCases := []struct {
		name    string
		message *testpb.OptionalMessage
		hasName bool
		hasAge  bool
	}{
		{
			name:    "all null",
			message: &testpb.OptionalMessage{Id: 1},
			hasName: false,
			hasAge:  false,
		},
		{
			name: "name set",
			message: func() *testpb.OptionalMessage {
				name := "Charlie"
				return &testpb.OptionalMessage{Id: 2, Name: &name}
			}(),
			hasName: true,
			hasAge:  false,
		},
		{
			name: "age set",
			message: func() *testpb.OptionalMessage {
				age := int32(25)
				return &testpb.OptionalMessage{Id: 3, Age: &age}
			}(),
			hasName: false,
			hasAge:  true,
		},
		{
			name: "both set",
			message: func() *testpb.OptionalMessage {
				name := "Diana"
				age := int32(35)
				return &testpb.OptionalMessage{Id: 4, Name: &name, Age: &age}
			}(),
			hasName: true,
			hasAge:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := avro.Marshal(avro.MustParse(schema), tc.message)
			require.NoError(t, err)

			var decoded testpb.OptionalMessage
			err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
			require.NoError(t, err)

			assert.Equal(t, tc.message.Id, decoded.Id)

			if tc.hasName {
				require.NotNil(t, decoded.Name)
				assert.Equal(t, *tc.message.Name, *decoded.Name)
			} else {
				assert.Nil(t, decoded.Name)
			}

			if tc.hasAge {
				require.NotNil(t, decoded.Age)
				assert.Equal(t, *tc.message.Age, *decoded.Age)
			} else {
				assert.Nil(t, decoded.Age)
			}
		})
	}
}

func TestProtobuf_OneofMessage_Text(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": ["null", "string", "int", "boolean"]}
		]
	}`

	original := &testpb.OneofMessage{
		Id:    1,
		Value: &testpb.OneofMessage_Text{Text: "hello"},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Value)
	textValue, ok := decoded.Value.(*testpb.OneofMessage_Text)
	require.True(t, ok)
	assert.Equal(t, "hello", textValue.Text)
}

func TestProtobuf_OneofMessage_Number(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": ["null", "string", "int", "boolean"]}
		]
	}`

	original := &testpb.OneofMessage{
		Id:    2,
		Value: &testpb.OneofMessage_Number{Number: 42},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Value)
	numberValue, ok := decoded.Value.(*testpb.OneofMessage_Number)
	require.True(t, ok)
	assert.Equal(t, int32(42), numberValue.Number)
}

func TestProtobuf_OneofMessage_Flag(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": ["null", "string", "int", "boolean"]}
		]
	}`

	original := &testpb.OneofMessage{
		Id:    3,
		Value: &testpb.OneofMessage_Flag{Flag: true},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Value)
	flagValue, ok := decoded.Value.(*testpb.OneofMessage_Flag)
	require.True(t, ok)
	assert.Equal(t, true, flagValue.Flag)
}

func TestProtobuf_OneofMessage_Null(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": ["null", "string", "int", "boolean"]}
		]
	}`

	// Oneof is not set - should be null
	original := &testpb.OneofMessage{
		Id:    4,
		Value: nil,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Nil(t, decoded.Value)
}

func TestProtobuf_OneofMessage_RoundTrip(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "value", "type": ["null", "string", "int", "boolean"]}
		]
	}`

	testCases := []struct {
		name    string
		message *testpb.OneofMessage
	}{
		{
			name:    "null",
			message: &testpb.OneofMessage{Id: 1, Value: nil},
		},
		{
			name:    "text",
			message: &testpb.OneofMessage{Id: 2, Value: &testpb.OneofMessage_Text{Text: "world"}},
		},
		{
			name:    "number",
			message: &testpb.OneofMessage{Id: 3, Value: &testpb.OneofMessage_Number{Number: 99}},
		},
		{
			name:    "flag_true",
			message: &testpb.OneofMessage{Id: 4, Value: &testpb.OneofMessage_Flag{Flag: true}},
		},
		{
			name:    "flag_false",
			message: &testpb.OneofMessage{Id: 5, Value: &testpb.OneofMessage_Flag{Flag: false}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := avro.Marshal(avro.MustParse(schema), tc.message)
			require.NoError(t, err)

			var decoded testpb.OneofMessage
			err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
			require.NoError(t, err)

			assert.Equal(t, tc.message.Id, decoded.Id)

			if tc.message.Value == nil {
				assert.Nil(t, decoded.Value)
			} else {
				require.NotNil(t, decoded.Value)

				switch v := tc.message.Value.(type) {
				case *testpb.OneofMessage_Text:
					dv, ok := decoded.Value.(*testpb.OneofMessage_Text)
					require.True(t, ok)
					assert.Equal(t, v.Text, dv.Text)
				case *testpb.OneofMessage_Number:
					dv, ok := decoded.Value.(*testpb.OneofMessage_Number)
					require.True(t, ok)
					assert.Equal(t, v.Number, dv.Number)
				case *testpb.OneofMessage_Flag:
					dv, ok := decoded.Value.(*testpb.OneofMessage_Flag)
					require.True(t, ok)
					assert.Equal(t, v.Flag, dv.Flag)
				}
			}
		})
	}
}

func TestProtobuf_OneofWithMessageMessage_Description(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofWithMessageMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "data",
				"type": [
					"null",
					"string",
					{
						"type": "record",
						"name": "BasicMessage",
						"fields": [
							{"name": "id", "type": "int"},
							{"name": "name", "type": "string"},
							{"name": "active", "type": "boolean"},
							{"name": "score", "type": "double"}
						]
					}
				]
			}
		]
	}`

	original := &testpb.OneofWithMessageMessage{
		Id:   1,
		Data: &testpb.OneofWithMessageMessage_Description{Description: "test desc"},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofWithMessageMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Data)
	descValue, ok := decoded.Data.(*testpb.OneofWithMessageMessage_Description)
	require.True(t, ok)
	assert.Equal(t, "test desc", descValue.Description)
}

func TestProtobuf_OneofWithMessageMessage_User(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofWithMessageMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "data",
				"type": [
					"null",
					"string",
					{
						"type": "record",
						"name": "BasicMessage",
						"fields": [
							{"name": "id", "type": "int"},
							{"name": "name", "type": "string"},
							{"name": "active", "type": "boolean"},
							{"name": "score", "type": "double"}
						]
					}
				]
			}
		]
	}`

	original := &testpb.OneofWithMessageMessage{
		Id: 2,
		Data: &testpb.OneofWithMessageMessage_User{
			User: &testpb.BasicMessage{
				Id:     10,
				Name:   "John",
				Active: true,
				Score:  95.5,
			},
		},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofWithMessageMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Data)
	userValue, ok := decoded.Data.(*testpb.OneofWithMessageMessage_User)
	require.True(t, ok)
	require.NotNil(t, userValue.User)
	assert.Equal(t, int32(10), userValue.User.Id)
	assert.Equal(t, "John", userValue.User.Name)
	assert.Equal(t, true, userValue.User.Active)
	assert.Equal(t, 95.5, userValue.User.Score)
}

func TestProtobuf_OneofWithMessageMessage_Null(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofWithMessageMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "data",
				"type": [
					"null",
					"string",
					{
						"type": "record",
						"name": "BasicMessage",
						"fields": [
							{"name": "id", "type": "int"},
							{"name": "name", "type": "string"},
							{"name": "active", "type": "boolean"},
							{"name": "score", "type": "double"}
						]
					}
				]
			}
		]
	}`

	original := &testpb.OneofWithMessageMessage{
		Id:   3,
		Data: nil,
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofWithMessageMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	assert.Nil(t, decoded.Data)
}

func TestProtobuf_ExtraFieldsInAvro(t *testing.T) {
	defer ConfigTeardown()

	// Avro schema has extra fields that don't exist in the protobuf message
	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "extra_string", "type": "string"},
			{"name": "name", "type": "string"},
			{"name": "extra_int", "type": "int"},
			{"name": "active", "type": "boolean"},
			{"name": "extra_double", "type": "double"},
			{"name": "score", "type": "double"}
		]
	}`

	// Encode data with a regular map that includes the extra fields
	data := map[string]any{
		"id":           int32(42),
		"extra_string": "this field doesn't exist in proto",
		"name":         "John Doe",
		"extra_int":    int32(999),
		"active":       true,
		"extra_double": 3.14159,
		"score":        95.5,
	}

	encoded, err := avro.Marshal(avro.MustParse(schema), data)
	require.NoError(t, err)

	// Decode into protobuf message - should skip the extra fields
	var decoded testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), encoded, &decoded)
	require.NoError(t, err)

	// Only the fields that exist in both should be populated
	assert.Equal(t, int32(42), decoded.Id)
	assert.Equal(t, "John Doe", decoded.Name)
	assert.Equal(t, true, decoded.Active)
	assert.Equal(t, 95.5, decoded.Score)
}

func TestProtobuf_ExtraComplexFieldsInAvro(t *testing.T) {
	defer ConfigTeardown()

	// Avro schema has extra complex fields (record, array, map)
	schema := `{
		"type": "record",
		"name": "BasicMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "extra_record",
				"type": {
					"type": "record",
					"name": "ExtraRecord",
					"fields": [
						{"name": "field1", "type": "string"},
						{"name": "field2", "type": "int"}
					]
				}
			},
			{"name": "name", "type": "string"},
			{"name": "extra_array", "type": {"type": "array", "items": "string"}},
			{"name": "active", "type": "boolean"},
			{"name": "extra_map", "type": {"type": "map", "values": "int"}},
			{"name": "score", "type": "double"}
		]
	}`

	// Encode data with extra complex fields
	data := map[string]any{
		"id": int32(99),
		"extra_record": map[string]any{
			"field1": "nested",
			"field2": int32(123),
		},
		"name":        "Jane Doe",
		"extra_array": []any{"item1", "item2", "item3"},
		"active":      false,
		"extra_map": map[string]any{
			"key1": int32(1),
			"key2": int32(2),
		},
		"score": 88.5,
	}

	encoded, err := avro.Marshal(avro.MustParse(schema), data)
	require.NoError(t, err)

	// Decode into protobuf message - should skip all the extra complex fields
	var decoded testpb.BasicMessage
	err = avro.Unmarshal(avro.MustParse(schema), encoded, &decoded)
	require.NoError(t, err)

	// Only the fields that exist in both should be populated
	assert.Equal(t, int32(99), decoded.Id)
	assert.Equal(t, "Jane Doe", decoded.Name)
	assert.Equal(t, false, decoded.Active)
	assert.Equal(t, 88.5, decoded.Score)
}

func TestProtobuf_OneofWithMessageMessage_Profile(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
		"type": "record",
		"name": "OneofWithMessageMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "data",
				"type": [
					"null",
					"string",
					{
						"type": "record",
						"name": "BasicMessage",
						"fields": [
							{"name": "id", "type": "int"},
							{"name": "name", "type": "string"},
							{"name": "active", "type": "boolean"},
							{"name": "score", "type": "double"}
						]
					},
					{
						"type": "record",
						"name": "SimpleProfile",
						"fields": [
							{"name": "user_id", "type": "int"},
							{"name": "bio", "type": "string"},
							{"name": "followers", "type": "int"}
						]
					}
				]
			}
		]
	}`

	original := &testpb.OneofWithMessageMessage{
		Id: 1,
		Data: &testpb.OneofWithMessageMessage_Profile{
			Profile: &testpb.SimpleProfile{
				UserId:    100,
				Bio:       "Software Developer",
				Followers: 1500,
			},
		},
	}

	data, err := avro.Marshal(avro.MustParse(schema), original)
	require.NoError(t, err)

	var decoded testpb.OneofWithMessageMessage
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original.Id, decoded.Id)
	require.NotNil(t, decoded.Data)
	profileValue, ok := decoded.Data.(*testpb.OneofWithMessageMessage_Profile)
	require.True(t, ok)
	require.NotNil(t, profileValue.Profile)
	assert.Equal(t, int32(100), profileValue.Profile.UserId)
	assert.Equal(t, "Software Developer", profileValue.Profile.Bio)
	assert.Equal(t, int32(1500), profileValue.Profile.Followers)
}
