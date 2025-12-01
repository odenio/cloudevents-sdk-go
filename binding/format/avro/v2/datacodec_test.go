/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package avro_test

import (
	"context"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"

	avrofmt "github.com/cloudevents/sdk-go/binding/format/avro/v2"
)

// TestRecord is a sample Avro record for testing
type TestRecord struct {
	Name  string `avro:"name"`
	Value int    `avro:"value"`
}

var testRecordSchema avro.Schema

func init() {
	var err error
	testRecordSchema, err = avro.Parse(`{
		"type": "record",
		"name": "TestRecord",
		"namespace": "test",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`)
	if err != nil {
		panic(err)
	}
}

// Implement SchemaProvider for TestRecord
func (t *TestRecord) AvroSchema() avro.Schema {
	return testRecordSchema
}

// Implement SchemaProvider for non-pointer TestRecord
func (t TestRecord) AvroSchemaValue() avro.Schema {
	return testRecordSchema
}

// schemaProviderRecord wraps TestRecord to provide schema
type schemaProviderRecord struct {
	TestRecord
}

func (s *schemaProviderRecord) AvroSchema() avro.Schema {
	return testRecordSchema
}

func TestDataCodecWithSchemaProvider(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	original := &schemaProviderRecord{
		TestRecord: TestRecord{
			Name:  "test-name",
			Value: 42,
		},
	}

	// Encode
	encoded, err := avrofmt.EncodeData(ctx, original)
	require.NoError(err)
	require.NotEmpty(encoded)

	// Decode
	decoded := &schemaProviderRecord{}
	err = avrofmt.DecodeData(ctx, encoded, decoded)
	require.NoError(err)

	require.Equal(original.Name, decoded.Name)
	require.Equal(original.Value, decoded.Value)
}

func TestDataCodecWithBytes(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	// When input is already bytes, it should be returned as-is
	original := []byte{0x01, 0x02, 0x03}
	encoded, err := avrofmt.EncodeData(ctx, original)
	require.NoError(err)
	require.Equal(original, encoded)
}

func TestDataCodecWithoutSchema(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	// Without a schema, encoding should fail
	type NoSchemaRecord struct {
		Field string
	}

	_, err := avrofmt.EncodeData(ctx, &NoSchemaRecord{Field: "test"})
	require.Error(err)
	require.Contains(err.Error(), "no schema available")
}

// TestSchemaRegistry implements SchemaRegistry for testing
type TestSchemaRegistry struct {
	schemas map[string]avro.Schema
}

func NewTestSchemaRegistry() *TestSchemaRegistry {
	return &TestSchemaRegistry{
		schemas: make(map[string]avro.Schema),
	}
}

func (r *TestSchemaRegistry) Register(typeName string, schema avro.Schema) {
	r.schemas[typeName] = schema
}

func (r *TestSchemaRegistry) GetSchema(v interface{}) (avro.Schema, error) {
	switch v.(type) {
	case *TestRecord, TestRecord:
		return testRecordSchema, nil
	}
	return nil, nil
}

func TestDataCodecWithSchemaRegistry(t *testing.T) {
	require := require.New(t)
	ctx := context.Background()

	// Set up schema registry
	registry := NewTestSchemaRegistry()
	avrofmt.SetSchemaRegistry(registry)
	defer avrofmt.SetSchemaRegistry(nil) // Clean up

	original := &TestRecord{
		Name:  "registry-test",
		Value: 100,
	}

	// Encode using registry
	encoded, err := avrofmt.EncodeData(ctx, original)
	require.NoError(err)
	require.NotEmpty(encoded)

	// Decode using registry
	decoded := &TestRecord{}
	err = avrofmt.DecodeData(ctx, encoded, decoded)
	require.NoError(err)

	require.Equal(original.Name, decoded.Name)
	require.Equal(original.Value, decoded.Value)
}

func TestContentTypeConstant(t *testing.T) {
	require.Equal(t, "application/avro", avrofmt.ContentTypeAvro)
}
