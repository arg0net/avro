# Custom Avro Marshaling

This document describes the custom marshaling feature that allows structs to implement their own Avro serialization logic.

## Overview

The `hamba/avro` library now supports custom marshaling through two interfaces:

- `RecordMarshaler` - for custom encoding
- `RecordUnmarshaler` - for custom decoding

These interfaces allow types to control their own Avro serialization while still integrating seamlessly with the library's encoding/decoding infrastructure.

## Interfaces

### RecordMarshaler

```go
type RecordMarshaler interface {
    MarshalAvro(w *Writer) error
}
```

Types implementing this interface can customize how they are encoded to Avro format. The `Writer` provides methods for writing all Avro primitive types:
- `WriteInt(int32)`
- `WriteLong(int64)`
- `WriteFloat(float32)`
- `WriteDouble(float64)`
- `WriteBool(bool)`
- `WriteString(string)`
- `WriteBytes([]byte)`

### RecordUnmarshaler

```go
type RecordUnmarshaler interface {
    UnmarshalAvro(r *Reader) error
}
```

Types implementing this interface can customize how they are decoded from Avro format. The `Reader` provides methods for reading all Avro primitive types:
- `ReadInt() int32`
- `ReadLong() int64`
- `ReadFloat() float32`
- `ReadDouble() float64`
- `ReadBool() bool`
- `ReadString() string`
- `ReadBytes() []byte`

## Features

### Works with Record Schemas Only

Custom marshaling is only applied to record schemas. For primitive types and other schema types, the standard marshaling logic is used.

### Supports Both Value and Pointer Receivers

You can implement the interfaces with either value or pointer receivers:

```go
// Value receiver for MarshalAvro
func (u User) MarshalAvro(w *avro.Writer) error { ... }

// Pointer receiver for UnmarshalAvro
func (u *User) UnmarshalAvro(r *avro.Reader) error { ... }
```

### No Internal Types Required

Both interfaces use only public types (`*Writer` and `*Reader`), so external packages can implement them without accessing any internal types from `hamba/avro`.

### Integrates with Existing APIs

Custom marshalers work seamlessly with all existing encoding/decoding APIs:
- `Marshal()` / `Unmarshal()`
- `Encoder.Encode()` / `Decoder.Decode()`
- Direct `Writer.WriteVal()` / `Reader.ReadVal()` calls

## Example Usage

### Basic Example

```go
type User struct {
    ID        int
    Name      string
    CreatedAt time.Time
}

// Implement custom marshaling
func (u User) MarshalAvro(w *avro.Writer) error {
    w.WriteInt(int32(u.ID))
    w.WriteString(u.Name)
    w.WriteLong(u.CreatedAt.Unix())
    return nil
}

// Implement custom unmarshaling
func (u *User) UnmarshalAvro(r *avro.Reader) error {
    u.ID = int(r.ReadInt())
    u.Name = r.ReadString()
    timestamp := r.ReadLong()
    u.CreatedAt = time.Unix(timestamp, 0).UTC()
    return nil
}

// Use with standard APIs
schema := `{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "created_at", "type": "long"}
    ]
}`

user := User{ID: 42, Name: "John", CreatedAt: time.Now()}

// Marshal
data, err := avro.Marshal(avro.MustParse(schema), user)

// Unmarshal
var decoded User
err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
```

### Handling Nested Structs

When a struct contains nested structs that also implement custom marshaling, you can delegate to the nested marshaler:

```go
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

type Employee struct {
    ID      int32
    Name    string
    Address Address
}

func (e Employee) MarshalAvro(w *avro.Writer) error {
    w.WriteInt(e.ID)
    w.WriteString(e.Name)
    // Delegate to nested struct's marshaler
    return e.Address.MarshalAvro(w)
}

func (e *Employee) UnmarshalAvro(r *avro.Reader) error {
    e.ID = r.ReadInt()
    e.Name = r.ReadString()
    // Delegate to nested struct's unmarshaler
    return e.Address.UnmarshalAvro(r)
}
```

**Schema for nested example:**
```json
{
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
}
```

### Handling Union Types (Nullable Fields)

Union types in Avro require writing a type index followed by the value. For nullable fields (union of `["null", "type"]`), use index 0 for null and 1 for the value:

```go
type Account struct {
    ID       int32
    Username string
    Email    *string // nullable - maps to ["null", "string"]
    Phone    *string // nullable - maps to ["null", "string"]
}

func (a Account) MarshalAvro(w *avro.Writer) error {
    w.WriteInt(a.ID)
    w.WriteString(a.Username)
    
    // Handle nullable email
    if a.Email == nil {
        w.WriteLong(0) // null type index
    } else {
        w.WriteLong(1)           // string type index
        w.WriteString(*a.Email)  // write the value
    }
    
    // Handle nullable phone
    if a.Phone == nil {
        w.WriteLong(0) // null type index
    } else {
        w.WriteLong(1)           // string type index
        w.WriteString(*a.Phone)  // write the value
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
```

**Schema for union example:**
```json
{
    "type": "record",
    "name": "Account",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "username", "type": "string"},
        {"name": "email", "type": ["null", "string"], "default": null},
        {"name": "phone", "type": ["null", "string"], "default": null}
    ]
}
```

**Important Union Notes:**
- Always write the type index first using `WriteLong()`
- Index corresponds to the position in the union type array (0-based)
- For `["null", "string"]`: index 0 = null, index 1 = string
- For `["string", "int"]`: index 0 = string, index 1 = int
- Only write the value if the type index is not null

### Combining Nested Structs and Unions

You can combine both patterns for nullable nested structs:

```go
type EmployeeWithOptionalAddress struct {
    ID      int32
    Name    string
    Address *Address  // nullable nested struct
}

func (e EmployeeWithOptionalAddress) MarshalAvro(w *avro.Writer) error {
    w.WriteInt(e.ID)
    w.WriteString(e.Name)
    
    // Handle nullable nested record
    if e.Address == nil {
        w.WriteLong(0) // null type index
    } else {
        w.WriteLong(1) // Address record type index
        // Delegate to nested struct's marshaler
        err := e.Address.MarshalAvro(w)
        if err != nil {
            return err
        }
    }
    return nil
}

func (e *EmployeeWithOptionalAddress) UnmarshalAvro(r *avro.Reader) error {
    e.ID = r.ReadInt()
    e.Name = r.ReadString()
    
    // Handle nullable nested record
    index := r.ReadLong()
    if index == 0 {
        e.Address = nil
    } else {
        e.Address = &Address{}
        err := e.Address.UnmarshalAvro(r)
        if err != nil {
            return err
        }
    }
    return nil
}
```

### Error Handling

Custom marshalers can return errors to indicate serialization failures:

```go
func (p Product) MarshalAvro(w *avro.Writer) error {
    if p.Price < 0 {
        return fmt.Errorf("invalid price: %f", p.Price)
    }
    w.WriteString(p.SKU)
    w.WriteDouble(p.Price)
    return nil
}
```

## Use Cases

Custom marshaling is useful for:

1. **Custom Type Conversions**: Converting between Go types and Avro types in specific ways (e.g., `time.Time` to Unix timestamp)

2. **Field Transformations**: Applying transformations during serialization (e.g., normalization, encryption)

3. **Compatibility Layers**: Maintaining compatibility with legacy data formats

4. **Performance Optimization**: Optimizing serialization for specific use cases

5. **Validation**: Adding validation logic during marshaling/unmarshaling

## Implementation Details

### Codec Selection

When encoding/decoding a struct, the library checks (in order):
1. Does the type implement `RecordMarshaler`/`RecordUnmarshaler`?
2. Does a pointer to the type implement the interfaces?
3. Fall back to standard struct marshaling

### Pointer Handling

For types where the pointer implements `RecordMarshaler` (pointer receiver methods), the library automatically handles taking the address when encoding value types.

### Performance

Custom marshalers use the same efficient `Writer` and `Reader` infrastructure as the standard codecs, so there is minimal performance overhead.

## Protobuf Message Support

The library automatically supports protobuf messages without requiring explicit marshaler implementation. When a type implements `proto.Message`, the library uses protobuf reflection to automatically serialize/deserialize the message according to the Avro schema.

### How It Works

- **Automatic Detection**: The library automatically detects types that implement `proto.Message` interface
- **Field Mapping**: Protobuf fields are mapped to Avro schema fields by name
- **Type Conversion**: Protobuf types are automatically converted to corresponding Avro types
- **Priority**: Protobuf detection occurs before checking for `RecordMarshaler`/`RecordUnmarshaler`

### Example Protobuf Definition

```protobuf
syntax = "proto3";

message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
  repeated string tags = 4;
}
```

### Corresponding Avro Schema

```json
{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "tags", "type": {"type": "array", "items": "string"}}
    ]
}
```

### Usage Example

```go
import (
    "github.com/hamba/avro/v2"
    pb "your/protobuf/package"
)

// Create protobuf message
user := &pb.User{
    Id:    42,
    Name:  "John Doe",
    Email: "john@example.com",
    Tags:  []string{"developer", "golang"},
}

// Marshal to Avro (works automatically!)
data, err := avro.Marshal(schema, user)
if err != nil {
    log.Fatal(err)
}

// Unmarshal from Avro
var decoded pb.User
err = avro.Unmarshal(schema, data, &decoded)
if err != nil {
    log.Fatal(err)
}
```

### Type Mapping

| Protobuf Type | Avro Type |
|--------------|-----------|
| int32, sint32, sfixed32 | int |
| int64, sint64, sfixed64 | long |
| uint32, fixed32 | int |
| uint64, fixed64 | long |
| float | float |
| double | double |
| bool | boolean |
| string | string |
| bytes | bytes |
| message | record |
| repeated T | array |
| map<string,V> | map |
| enum | int or string |

### Supported Features

- **Nested Messages**: Protobuf messages can contain other messages
- **Repeated Fields**: Protobuf repeated fields map to Avro arrays
- **Map Fields**: Protobuf maps map to Avro maps (keys must be strings)
- **Enum Fields**: Can be encoded as either int (enum number) or string (enum name)
- **All Numeric Types**: All protobuf integer and floating-point types are supported
- **Optional Fields**: Proto3 optional fields automatically map to Avro nullable unions
- **Oneof Fields**: Proto3 oneof fields map to Avro nullable unions

### Limitations

- Field names must match exactly between protobuf definition and Avro schema
- Map keys must be strings (protobuf limitation for complex key types)

### Nested Messages Example

```protobuf
message Article {
  int32 id = 1;
  string title = 2;
  User author = 3;
}
```

```json
{
    "type": "record",
    "name": "Article",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "title", "type": "string"},
        {
            "name": "author",
            "type": {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"}
                ]
            }
        }
    ]
}
```

This works automatically - the library recursively handles nested protobuf messages.

### Optional Fields Example

Proto3 optional fields are automatically detected and mapped to Avro nullable unions:

```protobuf
message Profile {
  int32 id = 1;
  optional string bio = 2;
  optional int32 age = 3;
}
```

```json
{
    "type": "record",
    "name": "Profile",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "bio", "type": ["null", "string"]},
        {"name": "age", "type": ["null", "int"]}
    ]
}
```

When optional fields are not set in the protobuf message, they are encoded as null in Avro. When decoding, null values result in the field not being set (using protobuf's field presence tracking).

```go
// Create message with optional fields
bio := "Software Engineer"
profile := &pb.Profile{
    Id:  1,
    Bio: &bio,
    // Age is not set (will be null)
}

// Encode and decode - Age remains unset
data, _ := avro.Marshal(schema, profile)
var decoded pb.Profile
avro.Unmarshal(schema, data, &decoded)
// decoded.Age will be nil
```

### Oneof Fields Example

Protobuf oneof fields are automatically mapped to Avro nullable unions. Oneof fields are always nullable since none of the options might be set:

```protobuf
message Event {
  int32 id = 1;
  oneof payload {
    string message = 2;
    int32 code = 3;
    bool flag = 4;
  }
}
```

```json
{
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "payload", "type": ["null", "string", "int", "boolean"]}
    ]
}
```

When encoding, the library automatically determines which field in the oneof is set and writes the corresponding union type. When no field is set, it writes null:

```go
// Oneof with message field set
event1 := &pb.Event{
    Id:      1,
    Payload: &pb.Event_Message{Message: "hello"},
}

// Oneof with code field set
event2 := &pb.Event{
    Id:      2,
    Payload: &pb.Event_Code{Code: 404},
}

// Oneof not set (null)
event3 := &pb.Event{
    Id:      3,
    Payload: nil,
}

// All encode and decode correctly
```

## Testing

The implementation includes comprehensive tests covering:
- Value and pointer receivers
- Error handling
- Nil pointer handling
- Round-trip encoding/decoding
- Integration with encoders/decoders
- External package usage
- Protobuf messages (basic, nested, arrays, maps, enums, optional fields, oneof fields)

See `codec_marshaler_avro_test.go`, `example_external_package_test.go`, and `codec_protobuf_test.go` for test examples.

