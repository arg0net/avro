package avro

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var protoMessageType = reflect2.TypeOfPtr((*proto.Message)(nil)).Elem()

// createDecoderOfProtobuf creates a decoder for protobuf messages.
// Returns nil if the type does not implement proto.Message or if schema is not a Record.
func createDecoderOfProtobuf(schema Schema, typ reflect2.Type) ValDecoder {
	if schema.Type() != Record {
		return nil
	}
	if typ.Implements(protoMessageType) {
		return &protobufCodec{typ: typ, schema: schema.(*RecordSchema)}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(protoMessageType) {
		return &referenceDecoder{
			&protobufCodec{typ: ptrType, schema: schema.(*RecordSchema)},
		}
	}
	return nil
}

// createEncoderOfProtobuf creates an encoder for protobuf messages.
// Returns nil if the type does not implement proto.Message or if schema is not a Record.
func createEncoderOfProtobuf(schema Schema, typ reflect2.Type) ValEncoder {
	if schema.Type() != Record {
		return nil
	}
	if typ.Implements(protoMessageType) {
		return &protobufCodec{typ: typ, schema: schema.(*RecordSchema)}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(protoMessageType) {
		return &protobufPtrCodec{typ: ptrType, elemTyp: typ, schema: schema.(*RecordSchema)}
	}
	return nil
}

type protobufCodec struct {
	typ    reflect2.Type
	schema *RecordSchema
}

func (c *protobufCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}

	msg := (obj).(proto.Message)
	msgReflect := msg.ProtoReflect()

	if err := c.decodeMessage(msgReflect, r); err != nil {
		r.ReportError("protobufCodec", err.Error())
	}
}

func (c *protobufCodec) decodeMessage(msgReflect protoreflect.Message, r *Reader) error {
	msgDesc := msgReflect.Descriptor()
	fields := msgDesc.Fields()

	// Iterate through Avro schema fields in order
	for _, avroField := range c.schema.Fields() {
		// Find corresponding protobuf field by name
		protoField := fields.ByName(protoreflect.Name(avroField.Name()))
		if protoField == nil {
			// Field not in protobuf message, skip it in the Avro data
			skipDecoder := createSkipDecoder(avroField.Type())
			skipDecoder.Decode(nil, r)
			if r.Error != nil {
				return r.Error
			}
			continue
		}

		// Read value from Avro and set it in protobuf message
		if err := c.decodeField(msgReflect, protoField, avroField.Type(), r); err != nil {
			return err
		}
		if r.Error != nil {
			return r.Error
		}
	}
	return nil
}

func (c *protobufCodec) decodeField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, r *Reader) error {
	if field.IsList() {
		return c.decodeListField(msg, field, avroSchema, r)
	}
	if field.IsMap() {
		return c.decodeMapField(msg, field, avroSchema, r)
	}

	// Handle optional fields with nullable unions
	if field.HasPresence() && avroSchema.Type() == Union {
		unionSchema := avroSchema.(*UnionSchema)
		if unionSchema.Nullable() {
			// Read union index
			index := r.ReadLong()
			if index == 0 {
				// Null value - clear the field (don't set it)
				msg.Clear(field)
				return nil
			}
			// Non-null value - read the actual value
			if index >= int64(len(unionSchema.Types())) {
				return fmt.Errorf("invalid union index %d", index)
			}
			actualSchema := unionSchema.Types()[index]
			val, err := c.decodeValue(msg, field, actualSchema, r)
			if err != nil {
				return err
			}
			if val.IsValid() {
				msg.Set(field, val)
			}
			return nil
		}
	}

	// Handle regular fields
	val, err := c.decodeValue(msg, field, avroSchema, r)
	if err != nil {
		return err
	}
	if val.IsValid() {
		msg.Set(field, val)
	}
	return nil
}

func (c *protobufCodec) decodeListField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, r *Reader) error {
	if avroSchema.Type() != Array {
		return fmt.Errorf("expected array schema for repeated field %s, got %s", field.Name(), avroSchema.Type())
	}
	arraySchema := avroSchema.(*ArraySchema)
	list := msg.Mutable(field).List()
	list.Truncate(0) // Clear existing values

	length := r.ReadLong()
	if length < 0 {
		length = -length
		_ = r.ReadLong() // block size, ignored
	}

	for length > 0 {
		for i := int64(0); i < length; i++ {
			val, err := c.decodeValue(msg, field, arraySchema.Items(), r)
			if err != nil {
				return err
			}
			list.Append(val)
		}
		length = r.ReadLong()
		if length < 0 {
			length = -length
			_ = r.ReadLong()
		}
	}
	return nil
}

func (c *protobufCodec) decodeMapField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, r *Reader) error {
	if avroSchema.Type() != Map {
		return fmt.Errorf("expected map schema for map field %s, got %s", field.Name(), avroSchema.Type())
	}
	mapSchema := avroSchema.(*MapSchema)
	mapVal := msg.Mutable(field).Map()
	// Clear existing values by iterating and removing
	mapVal.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
		mapVal.Clear(k)
		return true
	})

	length := r.ReadLong()
	if length < 0 {
		length = -length
		_ = r.ReadLong() // block size, ignored
	}

	for length > 0 {
		for i := int64(0); i < length; i++ {
			key := protoreflect.ValueOfString(r.ReadString())
			val, err := c.decodeValue(msg, field.MapValue(), mapSchema.Values(), r)
			if err != nil {
				return err
			}
			mapVal.Set(key.MapKey(), val)
		}
		length = r.ReadLong()
		if length < 0 {
			length = -length
			_ = r.ReadLong()
		}
	}
	return nil
}

func (c *protobufCodec) decodeValue(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, r *Reader) (protoreflect.Value, error) {
	kind := field.Kind()

	switch avroSchema.Type() {
	case Int:
		val := r.ReadInt()
		switch kind {
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			return protoreflect.ValueOfInt32(val), nil
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			return protoreflect.ValueOfUint32(uint32(val)), nil
		case protoreflect.EnumKind:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("cannot decode int to protobuf field %s of type %s", field.Name(), kind)
		}

	case Long:
		val := r.ReadLong()
		switch kind {
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			return protoreflect.ValueOfInt64(val), nil
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			return protoreflect.ValueOfUint64(uint64(val)), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("cannot decode long to protobuf field %s of type %s", field.Name(), kind)
		}

	case Float:
		val := r.ReadFloat()
		if kind != protoreflect.FloatKind {
			return protoreflect.Value{}, fmt.Errorf("cannot decode float to protobuf field %s of type %s", field.Name(), kind)
		}
		return protoreflect.ValueOfFloat32(val), nil

	case Double:
		val := r.ReadDouble()
		if kind != protoreflect.DoubleKind {
			return protoreflect.Value{}, fmt.Errorf("cannot decode double to protobuf field %s of type %s", field.Name(), kind)
		}
		return protoreflect.ValueOfFloat64(val), nil

	case Boolean:
		val := r.ReadBool()
		if kind != protoreflect.BoolKind {
			return protoreflect.Value{}, fmt.Errorf("cannot decode bool to protobuf field %s of type %s", field.Name(), kind)
		}
		return protoreflect.ValueOfBool(val), nil

	case String:
		val := r.ReadString()
		switch kind {
		case protoreflect.StringKind:
			return protoreflect.ValueOfString(val), nil
		case protoreflect.EnumKind:
			enumVal := field.Enum().Values().ByName(protoreflect.Name(val))
			if enumVal == nil {
				return protoreflect.Value{}, fmt.Errorf("unknown enum value %s for field %s", val, field.Name())
			}
			return protoreflect.ValueOfEnum(enumVal.Number()), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("cannot decode string to protobuf field %s of type %s", field.Name(), kind)
		}

	case Bytes:
		val := r.ReadBytes()
		if kind != protoreflect.BytesKind {
			return protoreflect.Value{}, fmt.Errorf("cannot decode bytes to protobuf field %s of type %s", field.Name(), kind)
		}
		return protoreflect.ValueOfBytes(val), nil

	case Record:
		if kind != protoreflect.MessageKind {
			return protoreflect.Value{}, fmt.Errorf("cannot decode record to protobuf field %s of type %s", field.Name(), kind)
		}
		nestedMsg := msg.NewField(field).Message()
		nestedCodec := &protobufCodec{
			typ:    nil, // Not needed for message-based decoding
			schema: avroSchema.(*RecordSchema),
		}
		if err := nestedCodec.decodeMessage(nestedMsg, r); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(nestedMsg), nil

	case Union:
		unionSchema := avroSchema.(*UnionSchema)
		index := r.ReadLong()
		if index < 0 || index >= int64(len(unionSchema.Types())) {
			return protoreflect.Value{}, fmt.Errorf("invalid union index %d", index)
		}
		selectedSchema := unionSchema.Types()[index]
		if selectedSchema.Type() == Null {
			return protoreflect.Value{}, nil // Return invalid value for null
		}
		return c.decodeValue(msg, field, selectedSchema, r)

	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported avro type %s for protobuf field %s", avroSchema.Type(), field.Name())
	}
}

func (c *protobufCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.Error = fmt.Errorf("cannot encode nil protobuf message")
		return
	}

	msg := (obj).(proto.Message)
	msgReflect := msg.ProtoReflect()

	if err := c.encodeMessage(msgReflect, w); err != nil {
		w.Error = err
	}
}

func (c *protobufCodec) encodeMessage(msgReflect protoreflect.Message, w *Writer) error {
	msgDesc := msgReflect.Descriptor()
	fields := msgDesc.Fields()

	// Iterate through Avro schema fields in order
	for _, avroField := range c.schema.Fields() {
		// Find corresponding protobuf field by name
		protoField := fields.ByName(protoreflect.Name(avroField.Name()))
		if protoField == nil {
			// Field not in protobuf message, use default value if available
			if avroField.HasDefault() {
				def := avroField.Default()
				if def == nil {
					// Write null for nullable union
					if avroField.Type().Type() == Union && avroField.Type().(*UnionSchema).Nullable() {
						w.WriteLong(0)
						continue
					}
				}
				// For other defaults, we'd need to encode them properly
				// For now, return an error
				return fmt.Errorf("field %s not found in protobuf message and no null default", avroField.Name())
			}
			return fmt.Errorf("required field %s not found in protobuf message", avroField.Name())
		}

		// Encode the field value
		if err := c.encodeField(msgReflect, protoField, avroField.Type(), w); err != nil {
			return err
		}
		if w.Error != nil {
			return w.Error
		}
	}
	return nil
}

func (c *protobufCodec) encodeField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, w *Writer) error {
	if field.IsList() {
		return c.encodeListField(msg, field, avroSchema, w)
	}
	if field.IsMap() {
		return c.encodeMapField(msg, field, avroSchema, w)
	}

	// Handle optional fields with nullable unions
	if field.HasPresence() && avroSchema.Type() == Union {
		unionSchema := avroSchema.(*UnionSchema)
		if unionSchema.Nullable() {
			// Check if the field is set
			if !msg.Has(field) {
				// Field not set - write null
				w.WriteLong(0)
				return nil
			}
			// Field is set - write non-null index and value
			// Find the non-null type in the union
			for i, t := range unionSchema.Types() {
				if t.Type() != Null {
					w.WriteLong(int64(i))
					val := msg.Get(field)
					return c.encodeValue(msg, field, val, t, w)
				}
			}
			return fmt.Errorf("no non-null type found in union for field %s", field.Name())
		}
	}

	val := msg.Get(field)
	return c.encodeValue(msg, field, val, avroSchema, w)
}

func (c *protobufCodec) encodeListField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, w *Writer) error {
	if avroSchema.Type() != Array {
		return fmt.Errorf("expected array schema for repeated field %s, got %s", field.Name(), avroSchema.Type())
	}
	arraySchema := avroSchema.(*ArraySchema)
	list := msg.Get(field).List()
	length := list.Len()

	if length == 0 {
		w.WriteLong(0)
		return nil
	}

	w.WriteLong(int64(length))
	for i := 0; i < length; i++ {
		val := list.Get(i)
		if err := c.encodeValue(msg, field, val, arraySchema.Items(), w); err != nil {
			return err
		}
	}
	w.WriteLong(0)
	return nil
}

func (c *protobufCodec) encodeMapField(msg protoreflect.Message, field protoreflect.FieldDescriptor, avroSchema Schema, w *Writer) error {
	if avroSchema.Type() != Map {
		return fmt.Errorf("expected map schema for map field %s, got %s", field.Name(), avroSchema.Type())
	}
	mapSchema := avroSchema.(*MapSchema)
	mapVal := msg.Get(field).Map()
	length := mapVal.Len()

	if length == 0 {
		w.WriteLong(0)
		return nil
	}

	w.WriteLong(int64(length))
	var encodeErr error
	mapVal.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		w.WriteString(k.String())
		if err := c.encodeValue(msg, field.MapValue(), v, mapSchema.Values(), w); err != nil {
			encodeErr = err
			return false
		}
		return true
	})
	if encodeErr != nil {
		return encodeErr
	}
	w.WriteLong(0)
	return nil
}

func (c *protobufCodec) encodeValue(msg protoreflect.Message, field protoreflect.FieldDescriptor, val protoreflect.Value, avroSchema Schema, w *Writer) error {
	kind := field.Kind()

	switch avroSchema.Type() {
	case Int:
		switch kind {
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			w.WriteInt(int32(val.Int()))
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			w.WriteInt(int32(val.Uint()))
		case protoreflect.EnumKind:
			w.WriteInt(int32(val.Enum()))
		default:
			return fmt.Errorf("cannot encode protobuf field %s of type %s to int", field.Name(), kind)
		}

	case Long:
		switch kind {
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			w.WriteLong(val.Int())
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			w.WriteLong(int64(val.Uint()))
		default:
			return fmt.Errorf("cannot encode protobuf field %s of type %s to long", field.Name(), kind)
		}

	case Float:
		if kind != protoreflect.FloatKind {
			return fmt.Errorf("cannot encode protobuf field %s of type %s to float", field.Name(), kind)
		}
		w.WriteFloat(float32(val.Float()))

	case Double:
		if kind != protoreflect.DoubleKind {
			return fmt.Errorf("cannot encode protobuf field %s of type %s to double", field.Name(), kind)
		}
		w.WriteDouble(val.Float())

	case Boolean:
		if kind != protoreflect.BoolKind {
			return fmt.Errorf("cannot encode protobuf field %s of type %s to bool", field.Name(), kind)
		}
		w.WriteBool(val.Bool())

	case String:
		switch kind {
		case protoreflect.StringKind:
			w.WriteString(val.String())
		case protoreflect.EnumKind:
			enumVal := field.Enum().Values().ByNumber(val.Enum())
			if enumVal == nil {
				return fmt.Errorf("invalid enum number %d for field %s", val.Enum(), field.Name())
			}
			w.WriteString(string(enumVal.Name()))
		default:
			return fmt.Errorf("cannot encode protobuf field %s of type %s to string", field.Name(), kind)
		}

	case Bytes:
		if kind != protoreflect.BytesKind {
			return fmt.Errorf("cannot encode protobuf field %s of type %s to bytes", field.Name(), kind)
		}
		w.WriteBytes(val.Bytes())

	case Record:
		if kind != protoreflect.MessageKind {
			return fmt.Errorf("cannot encode protobuf field %s of type %s to record", field.Name(), kind)
		}
		nestedMsgReflect := val.Message()
		nestedCodec := &protobufCodec{
			typ:    nil, // Will be set when needed
			schema: avroSchema.(*RecordSchema),
		}
		// Encode the nested message directly using its reflection
		if err := nestedCodec.encodeMessage(nestedMsgReflect, w); err != nil {
			return err
		}

	case Union:
		unionSchema := avroSchema.(*UnionSchema)
		// Check if value is set (for optional fields)
		if !msg.Has(field) {
			// Field is not set, write null if union is nullable
			if unionSchema.Nullable() {
				w.WriteLong(0) // null type index
				return nil
			}
			return fmt.Errorf("field %s is not set and union is not nullable", field.Name())
		}
		// Find non-null type in union and write it
		for i, t := range unionSchema.Types() {
			if t.Type() != Null {
				w.WriteLong(int64(i))
				return c.encodeValue(msg, field, val, t, w)
			}
		}
		return fmt.Errorf("no non-null type found in union for field %s", field.Name())

	default:
		return fmt.Errorf("unsupported avro type %s for protobuf field %s", avroSchema.Type(), field.Name())
	}
	return nil
}

// protobufPtrCodec is used when a value type's pointer implements proto.Message
type protobufPtrCodec struct {
	typ     reflect2.Type
	elemTyp reflect2.Type
	schema  *RecordSchema
}

func (c *protobufPtrCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	// ptr points to the struct value, we need to pass the pointer (ptr itself)
	// to the encoder since proto.Message expects a pointer receiver
	codec := &protobufCodec{typ: c.typ, schema: c.schema}
	codec.Encode(unsafe.Pointer(&ptr), w)
}
