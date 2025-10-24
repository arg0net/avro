# Using Protobuf and Custom Marshaling with OCF

The OCF (Object Container Files) package fully supports both Protobuf messages and custom Avro marshalers through the standard avro encoding/decoding configuration.

## Protobuf Support

Protobuf messages work seamlessly with OCF encoders and decoders. No special configuration is needed:

```go
import (
    "github.com/hamba/avro/v2/ocf"
    testpb "github.com/hamba/avro/v2/testdata/protobuf"
)

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

// Encode protobuf messages to OCF
buf := &bytes.Buffer{}
enc, err := ocf.NewEncoder(schema, buf)
if err != nil {
    panic(err)
}

msg := &testpb.BasicMessage{
    Id:     42,
    Name:   "test",
    Active: true,
    Score:  99.5,
}

if err := enc.Encode(msg); err != nil {
    panic(err)
}
enc.Close()

// Decode protobuf messages from OCF
dec, err := ocf.NewDecoder(buf)
if err != nil {
    panic(err)
}

for dec.HasNext() {
    var decoded testpb.BasicMessage
    if err := dec.Decode(&decoded); err != nil {
        panic(err)
    }
    // Use decoded message
}
```

## Custom Marshaler Support

Custom Avro marshalers also work with OCF:

```go
type CustomValue struct {
    Value string
}

func (c CustomValue) MarshalAvro(w *avro.Writer) error {
    w.WriteString("custom_" + c.Value)
    return nil
}

func (c *CustomValue) UnmarshalAvro(r *avro.Reader) error {
    s := r.ReadString()
    if strings.HasPrefix(s, "custom_") {
        c.Value = s[7:]
    } else {
        c.Value = s
    }
    return nil
}

// Use with OCF
schema := `{
    "type": "record",
    "name": "CustomValue",
    "fields": [{"name": "value", "type": "string"}]
}`

buf := &bytes.Buffer{}
enc, err := ocf.NewEncoder(schema, buf)
if err != nil {
    panic(err)
}

original := CustomValue{Value: "test"}
if err := enc.Encode(original); err != nil {
    panic(err)
}
enc.Close()
```

## Using Custom Config

If you need to pass custom configuration (e.g., for buffer sizes or other options), use the `WithEncodingConfig` and `WithDecoderConfig` options:

```go
import "github.com/hamba/avro/v2"

// Create custom config
cfg := avro.Config{
    MaxByteSliceSize: 2000000,
}.Freeze()

// Use with encoder
enc, err := ocf.NewEncoder(schema, buf, ocf.WithEncodingConfig(cfg))

// Use with decoder
dec, err := ocf.NewDecoder(buf, ocf.WithDecoderConfig(cfg))
```

## Compression

Protobuf and custom marshalers work with all OCF compression codecs:

```go
// With Deflate compression
enc, err := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.Deflate))

// With Snappy compression
enc, err := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.Snappy))

// With ZStandard compression
enc, err := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.ZStandard))
```

## Implementation Details

The OCF package uses the standard `avro.API` interface for encoding and decoding, which means:

1. **Protobuf messages** are automatically detected and handled by the protobuf codec
2. **Custom marshalers** are automatically detected if they implement the `MarshalAvro` and `UnmarshalAvro` interfaces
3. All standard avro features (schema evolution, compression, etc.) work transparently with both

The key insight is that OCF is just a container format that wraps the standard Avro encoding. Since the standard Avro encoder/decoder already supports protobuf and custom marshaling, OCF inherits this support automatically.

