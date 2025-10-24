package avro_test

import (
	"fmt"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file demonstrates that external packages can implement
// AvroMarshaler and AvroUnmarshaler using only public types.
// No internal types from hamba/avro are required.

// Product is a type from a hypothetical external package that wants
// to control its own Avro serialization format.
type Product struct {
	SKU      string
	Price    float64
	Quantity int
}

// MarshalAvro uses only public avro.Writer methods.
// No internal types are needed.
func (p Product) MarshalAvro(w *avro.Writer) error {
	// Custom serialization: SKU, then price as double, then quantity as int
	w.WriteString(p.SKU)
	w.WriteDouble(p.Price)
	w.WriteInt(int32(p.Quantity))
	return nil
}

// UnmarshalAvro uses only public avro.Reader methods.
// No internal types are needed.
func (p *Product) UnmarshalAvro(r *avro.Reader) error {
	// Custom deserialization matching the marshal format
	p.SKU = r.ReadString()
	p.Price = r.ReadDouble()
	p.Quantity = int(r.ReadInt())
	return nil
}

// TestExternalPackageImplementation verifies that external packages
// can implement the marshaling interfaces using only public APIs.
func TestExternalPackageImplementation(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "sku", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "quantity", "type": "int"}
		]
	}`

	product := Product{
		SKU:      "PROD-123",
		Price:    29.99,
		Quantity: 100,
	}

	// Marshal using custom implementation
	data, err := avro.Marshal(avro.MustParse(schema), product)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Unmarshal using custom implementation
	var decoded Product
	err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, product.SKU, decoded.SKU)
	assert.Equal(t, product.Price, decoded.Price)
	assert.Equal(t, product.Quantity, decoded.Quantity)
}

// TestMultipleRecordsWithCustomMarshaling tests that custom marshaling
// works correctly with multiple records.
func TestMultipleRecordsWithCustomMarshaling(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "sku", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "quantity", "type": "int"}
		]
	}`

	products := []Product{
		{SKU: "A001", Price: 10.50, Quantity: 50},
		{SKU: "B002", Price: 25.00, Quantity: 30},
		{SKU: "C003", Price: 99.99, Quantity: 5},
	}

	// Encode and decode each product independently
	for i, p := range products {
		data, err := avro.Marshal(avro.MustParse(schema), p)
		require.NoError(t, err)

		var decoded Product
		err = avro.Unmarshal(avro.MustParse(schema), data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, products[i].SKU, decoded.SKU, "product %d SKU mismatch", i)
		assert.Equal(t, products[i].Price, decoded.Price, "product %d price mismatch", i)
		assert.Equal(t, products[i].Quantity, decoded.Quantity, "product %d quantity mismatch", i)
	}
}

// ExampleProduct demonstrates custom marshaling for a product type
// that can be used by external packages.
func ExampleProduct() {
	schema := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "sku", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "quantity", "type": "int"}
		]
	}`

	product := Product{
		SKU:      "WIDGET-500",
		Price:    19.99,
		Quantity: 250,
	}

	// Encode
	data, _ := avro.Marshal(avro.MustParse(schema), product)

	// Decode
	var decoded Product
	_ = avro.Unmarshal(avro.MustParse(schema), data, &decoded)

	fmt.Printf("SKU: %s, Price: $%.2f, Qty: %d\n",
		decoded.SKU, decoded.Price, decoded.Quantity)

	// Output:
	// SKU: WIDGET-500, Price: $19.99, Qty: 250
}
