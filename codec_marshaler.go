package avro

import (
	"encoding"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	textMarshalerType   = reflect2.TypeOfPtr((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerType = reflect2.TypeOfPtr((*encoding.TextUnmarshaler)(nil)).Elem()
	avroMarshalerType   = reflect2.TypeOfPtr((*RecordMarshaler)(nil)).Elem()
	avroUnmarshalerType = reflect2.TypeOfPtr((*RecordUnmarshaler)(nil)).Elem()
)

func createDecoderOfMarshaler(schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Implements(textUnmarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{typ}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(textUnmarshalerType) && schema.Type() == String {
		return &referenceDecoder{
			&textMarshalerCodec{ptrType},
		}
	}
	return nil
}

func createEncoderOfMarshaler(schema Schema, typ reflect2.Type) ValEncoder {
	if typ.Implements(textMarshalerType) && schema.Type() == String {
		return &textMarshalerCodec{
			typ: typ,
		}
	}
	return nil
}

type textMarshalerCodec struct {
	typ reflect2.Type
}

func (c textMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(encoding.TextUnmarshaler)
	b := r.ReadBytes()
	err := unmarshaler.UnmarshalText(b)
	if err != nil {
		r.ReportError("textMarshalerCodec", err.Error())
	}
}

func (c textMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.WriteBytes(nil)
		return
	}
	marshaler := (obj).(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}
	w.WriteBytes(b)
}

// RecordMarshaler is the interface implemented by types that can marshal themselves to Avro.
type RecordMarshaler interface {
	MarshalAvro(w *Writer) error
}

// RecordUnmarshaler is the interface implemented by types that can unmarshal an Avro
// description of themselves.
type RecordUnmarshaler interface {
	UnmarshalAvro(r *Reader) error
}

func createDecoderOfAvroMarshaler(schema Schema, typ reflect2.Type) ValDecoder {
	if schema.Type() != Record {
		return nil
	}
	if typ.Implements(avroUnmarshalerType) {
		return &avroMarshalerCodec{typ: typ}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(avroUnmarshalerType) {
		return &referenceDecoder{
			&avroMarshalerCodec{typ: ptrType},
		}
	}
	return nil
}

func createEncoderOfAvroMarshaler(schema Schema, typ reflect2.Type) ValEncoder {
	if schema.Type() != Record {
		return nil
	}
	if typ.Implements(avroMarshalerType) {
		return &avroMarshalerCodec{typ: typ}
	}
	ptrType := reflect2.PtrTo(typ)
	if ptrType.Implements(avroMarshalerType) {
		return &avroMarshalerPtrCodec{typ: ptrType, elemTyp: typ}
	}
	return nil
}

type avroMarshalerCodec struct {
	typ reflect2.Type
}

func (c *avroMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := c.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(RecordUnmarshaler)
	err := unmarshaler.UnmarshalAvro(r)
	if err != nil {
		r.ReportError("avroMarshalerCodec", err.Error())
	}
}

func (c *avroMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := c.typ.UnsafeIndirect(ptr)
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.Error = nil
		return
	}
	marshaler := (obj).(RecordMarshaler)
	err := marshaler.MarshalAvro(w)
	if err != nil {
		w.Error = err
	}
}

// avroMarshalerPtrCodec is used when a value type's pointer implements AvroMarshaler
type avroMarshalerPtrCodec struct {
	typ     reflect2.Type // pointer type that implements AvroMarshaler
	elemTyp reflect2.Type // element type (the actual struct)
}

func (c *avroMarshalerPtrCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	// ptr points to the struct value, we need to pass the pointer (ptr itself)
	// to the marshaler since it expects a pointer receiver
	marshaler := c.typ.UnsafeIndirect(unsafe.Pointer(&ptr)).(RecordMarshaler)
	err := marshaler.MarshalAvro(w)
	if err != nil {
		w.Error = err
	}
}
