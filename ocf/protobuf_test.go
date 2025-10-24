package ocf_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	testpb "github.com/hamba/avro/v2/testdata/protobuf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_Protobuf(t *testing.T) {
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

	// Create test data
	original := &testpb.BasicMessage{
		Id:     42,
		Name:   "test message",
		Active: true,
		Score:  99.5,
	}

	// Encode with OCF
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(original)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Decode with OCF
	dec, err := ocf.NewDecoder(buf)
	require.NoError(t, err)

	require.True(t, dec.HasNext())
	var decoded testpb.BasicMessage
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Active, decoded.Active)
	assert.Equal(t, original.Score, decoded.Score)

	require.False(t, dec.HasNext())
	require.NoError(t, dec.Error())
}

func TestEncoder_Protobuf_Multiple(t *testing.T) {
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

	// Create test data
	messages := []*testpb.BasicMessage{
		{Id: 1, Name: "first", Active: true, Score: 10.5},
		{Id: 2, Name: "second", Active: false, Score: 20.5},
		{Id: 3, Name: "third", Active: true, Score: 30.5},
	}

	// Encode with OCF
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	for _, msg := range messages {
		err = enc.Encode(msg)
		require.NoError(t, err)
	}

	err = enc.Close()
	require.NoError(t, err)

	// Decode with OCF
	dec, err := ocf.NewDecoder(buf)
	require.NoError(t, err)

	var decoded []*testpb.BasicMessage
	for dec.HasNext() {
		var msg testpb.BasicMessage
		err = dec.Decode(&msg)
		require.NoError(t, err)
		decoded = append(decoded, &msg)
	}

	require.NoError(t, dec.Error())
	require.Len(t, decoded, 3)

	// Verify
	for i, original := range messages {
		assert.Equal(t, original.Id, decoded[i].Id)
		assert.Equal(t, original.Name, decoded[i].Name)
		assert.Equal(t, original.Active, decoded[i].Active)
		assert.Equal(t, original.Score, decoded[i].Score)
	}
}

func TestEncoder_Protobuf_WithCompression(t *testing.T) {
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

	// Create test data
	original := &testpb.BasicMessage{
		Id:     42,
		Name:   "test message",
		Active: true,
		Score:  99.5,
	}

	// Encode with OCF with deflate compression
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.Deflate))
	require.NoError(t, err)

	err = enc.Encode(original)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Decode with OCF
	dec, err := ocf.NewDecoder(buf)
	require.NoError(t, err)

	require.True(t, dec.HasNext())
	var decoded testpb.BasicMessage
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Active, decoded.Active)
	assert.Equal(t, original.Score, decoded.Score)
}

func TestEncoder_Protobuf_Oneof(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "OneofMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{
				"name": "value",
				"type": ["null", "string", "int", "boolean"]
			}
		]
	}`

	// Test with string variant
	originalString := &testpb.OneofMessage{
		Id:    1,
		Value: &testpb.OneofMessage_Text{Text: "test"},
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(originalString)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Decode
	dec, err := ocf.NewDecoder(buf)
	require.NoError(t, err)

	require.True(t, dec.HasNext())
	var decodedString testpb.OneofMessage
	err = dec.Decode(&decodedString)
	require.NoError(t, err)

	assert.Equal(t, originalString.Id, decodedString.Id)
	textValue, ok := decodedString.Value.(*testpb.OneofMessage_Text)
	require.True(t, ok)
	assert.Equal(t, "test", textValue.Text)

	// Test with number variant
	originalNumber := &testpb.OneofMessage{
		Id:    2,
		Value: &testpb.OneofMessage_Number{Number: 42},
	}

	buf2 := &bytes.Buffer{}
	enc2, err := ocf.NewEncoder(schema, buf2)
	require.NoError(t, err)

	err = enc2.Encode(originalNumber)
	require.NoError(t, err)

	err = enc2.Close()
	require.NoError(t, err)

	// Decode
	dec2, err := ocf.NewDecoder(buf2)
	require.NoError(t, err)

	require.True(t, dec2.HasNext())
	var decodedNumber testpb.OneofMessage
	err = dec2.Decode(&decodedNumber)
	require.NoError(t, err)

	assert.Equal(t, originalNumber.Id, decodedNumber.Id)
	numberValue, ok := decodedNumber.Value.(*testpb.OneofMessage_Number)
	require.True(t, ok)
	assert.Equal(t, int32(42), numberValue.Number)
}

type CustomMarshalerValue struct {
	Value string
}

func (c CustomMarshalerValue) MarshalAvro(w *avro.Writer) error {
	w.WriteString("marshaled_" + c.Value)
	return nil
}

func (c *CustomMarshalerValue) UnmarshalAvro(r *avro.Reader) error {
	s := r.ReadString()
	if len(s) > 10 && s[:10] == "marshaled_" {
		c.Value = s[10:]
	} else {
		c.Value = s
	}
	return nil
}

func TestEncoder_CustomMarshaler(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "CustomMarshalerValue",
		"fields": [
			{"name": "value", "type": "string"}
		]
	}`

	// Create test data
	original := CustomMarshalerValue{Value: "test"}

	// Encode with OCF
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(original)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Decode with OCF
	dec, err := ocf.NewDecoder(buf)
	require.NoError(t, err)

	require.True(t, dec.HasNext())
	var decoded CustomMarshalerValue
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Value, decoded.Value)

	require.False(t, dec.HasNext())
	require.NoError(t, dec.Error())
}

func TestEncoder_Protobuf_WithEncodingConfig(t *testing.T) {
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

	// Create test data with a large name to test MaxByteSliceSize
	original := &testpb.BasicMessage{
		Id:     42,
		Name:   "test message with some longer content to demonstrate config support",
		Active: true,
		Score:  99.5,
	}

	// Create a custom config
	cfg := avro.Config{MaxByteSliceSize: 1000}.Freeze()

	// Encode with OCF
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf, ocf.WithEncodingConfig(cfg))
	require.NoError(t, err)

	err = enc.Encode(original)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Decode with OCF using the same config
	dec, err := ocf.NewDecoder(buf, ocf.WithDecoderConfig(cfg))
	require.NoError(t, err)

	require.True(t, dec.HasNext())
	var decoded testpb.BasicMessage
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.Id, decoded.Id)
	assert.Equal(t, original.Name, decoded.Name)
	assert.Equal(t, original.Active, decoded.Active)
	assert.Equal(t, original.Score, decoded.Score)
}
