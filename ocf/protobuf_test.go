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

func TestEncoder_Protobuf_OneofWithNestedStructs(t *testing.T) {
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

	tests := []struct {
		name     string
		message  *testpb.OneofWithMessageMessage
		validate func(t *testing.T, decoded *testpb.OneofWithMessageMessage)
	}{
		{
			name: "string variant",
			message: &testpb.OneofWithMessageMessage{
				Id:   1,
				Data: &testpb.OneofWithMessageMessage_Description{Description: "test description"},
			},
			validate: func(t *testing.T, decoded *testpb.OneofWithMessageMessage) {
				assert.Equal(t, int32(1), decoded.Id)
				desc, ok := decoded.Data.(*testpb.OneofWithMessageMessage_Description)
				require.True(t, ok, "expected Description variant")
				assert.Equal(t, "test description", desc.Description)
			},
		},
		{
			name: "BasicMessage variant",
			message: &testpb.OneofWithMessageMessage{
				Id: 2,
				Data: &testpb.OneofWithMessageMessage_User{
					User: &testpb.BasicMessage{
						Id:     10,
						Name:   "John Doe",
						Active: true,
						Score:  95.5,
					},
				},
			},
			validate: func(t *testing.T, decoded *testpb.OneofWithMessageMessage) {
				assert.Equal(t, int32(2), decoded.Id)
				user, ok := decoded.Data.(*testpb.OneofWithMessageMessage_User)
				require.True(t, ok, "expected User variant")
				require.NotNil(t, user.User)
				assert.Equal(t, int32(10), user.User.Id)
				assert.Equal(t, "John Doe", user.User.Name)
				assert.Equal(t, true, user.User.Active)
				assert.Equal(t, 95.5, user.User.Score)
			},
		},
		{
			name: "SimpleProfile variant",
			message: &testpb.OneofWithMessageMessage{
				Id: 3,
				Data: &testpb.OneofWithMessageMessage_Profile{
					Profile: &testpb.SimpleProfile{
						UserId:    100,
						Bio:       "Software Developer",
						Followers: 1500,
					},
				},
			},
			validate: func(t *testing.T, decoded *testpb.OneofWithMessageMessage) {
				assert.Equal(t, int32(3), decoded.Id)
				profile, ok := decoded.Data.(*testpb.OneofWithMessageMessage_Profile)
				require.True(t, ok, "expected Profile variant")
				require.NotNil(t, profile.Profile)
				assert.Equal(t, int32(100), profile.Profile.UserId)
				assert.Equal(t, "Software Developer", profile.Profile.Bio)
				assert.Equal(t, int32(1500), profile.Profile.Followers)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode with OCF
			buf := &bytes.Buffer{}
			enc, err := ocf.NewEncoder(schema, buf)
			require.NoError(t, err)

			err = enc.Encode(tt.message)
			require.NoError(t, err)

			err = enc.Close()
			require.NoError(t, err)

			// Decode with OCF
			dec, err := ocf.NewDecoder(buf)
			require.NoError(t, err)

			require.True(t, dec.HasNext())
			var decoded testpb.OneofWithMessageMessage
			err = dec.Decode(&decoded)
			require.NoError(t, err)

			// Validate
			tt.validate(t, &decoded)

			require.False(t, dec.HasNext())
			require.NoError(t, dec.Error())
		})
	}
}

func TestEncoder_Protobuf_OneofWithNestedStructs_Multiple(t *testing.T) {
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

	// Create multiple messages with different variants
	messages := []*testpb.OneofWithMessageMessage{
		{
			Id:   1,
			Data: &testpb.OneofWithMessageMessage_Description{Description: "first"},
		},
		{
			Id: 2,
			Data: &testpb.OneofWithMessageMessage_User{
				User: &testpb.BasicMessage{Id: 10, Name: "User 1", Active: true, Score: 90.0},
			},
		},
		{
			Id: 3,
			Data: &testpb.OneofWithMessageMessage_Profile{
				Profile: &testpb.SimpleProfile{
					UserId:    200,
					Bio:       "Data Scientist",
					Followers: 2500,
				},
			},
		},
		{
			Id:   4,
			Data: &testpb.OneofWithMessageMessage_Description{Description: "second"},
		},
		{
			Id: 5,
			Data: &testpb.OneofWithMessageMessage_User{
				User: &testpb.BasicMessage{Id: 40, Name: "User 2", Active: false, Score: 75.5},
			},
		},
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

	var decoded []*testpb.OneofWithMessageMessage
	for dec.HasNext() {
		var msg testpb.OneofWithMessageMessage
		err = dec.Decode(&msg)
		require.NoError(t, err)
		decoded = append(decoded, &msg)
	}

	require.NoError(t, dec.Error())
	require.Len(t, decoded, 5)

	// Verify first message (string variant)
	assert.Equal(t, int32(1), decoded[0].Id)
	desc1, ok := decoded[0].Data.(*testpb.OneofWithMessageMessage_Description)
	require.True(t, ok)
	assert.Equal(t, "first", desc1.Description)

	// Verify second message (BasicMessage variant)
	assert.Equal(t, int32(2), decoded[1].Id)
	user1, ok := decoded[1].Data.(*testpb.OneofWithMessageMessage_User)
	require.True(t, ok)
	assert.Equal(t, int32(10), user1.User.Id)
	assert.Equal(t, "User 1", user1.User.Name)

	// Verify third message (SimpleProfile variant)
	assert.Equal(t, int32(3), decoded[2].Id)
	profile1, ok := decoded[2].Data.(*testpb.OneofWithMessageMessage_Profile)
	require.True(t, ok)
	assert.Equal(t, int32(200), profile1.Profile.UserId)
	assert.Equal(t, "Data Scientist", profile1.Profile.Bio)
	assert.Equal(t, int32(2500), profile1.Profile.Followers)

	// Verify fourth message (string variant)
	assert.Equal(t, int32(4), decoded[3].Id)
	desc2, ok := decoded[3].Data.(*testpb.OneofWithMessageMessage_Description)
	require.True(t, ok)
	assert.Equal(t, "second", desc2.Description)

	// Verify fifth message (BasicMessage variant)
	assert.Equal(t, int32(5), decoded[4].Id)
	user2, ok := decoded[4].Data.(*testpb.OneofWithMessageMessage_User)
	require.True(t, ok)
	assert.Equal(t, int32(40), user2.User.Id)
	assert.Equal(t, "User 2", user2.User.Name)
}
