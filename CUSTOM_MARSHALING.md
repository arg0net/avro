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

## Testing

The implementation includes comprehensive tests covering:
- Value and pointer receivers
- Error handling
- Nil pointer handling
- Round-trip encoding/decoding
- Integration with encoders/decoders
- External package usage

See `codec_marshaler_avro_test.go` and `example_external_package_test.go` for test examples.

