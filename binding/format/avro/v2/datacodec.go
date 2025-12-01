/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package avro

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/cloudevents/sdk-go/v2/event/datacodec"
)

const (
	// ContentTypeAvro indicates that the data attribute is an Avro message.
	ContentTypeAvro = "application/avro"
)

func init() {
	datacodec.AddDecoder(ContentTypeAvro, DecodeData)
	datacodec.AddEncoder(ContentTypeAvro, EncodeData)
	datacodec.AddStructuredSuffixDecoder("avro", DecodeData)
	datacodec.AddStructuredSuffixEncoder("avro", EncodeData)
}

// SchemaRegistry is an interface for looking up Avro schemas.
// Users should implement this interface to provide schema resolution for their data types.
type SchemaRegistry interface {
	// GetSchema returns the Avro schema for the given type.
	GetSchema(v interface{}) (avro.Schema, error)
}

// defaultRegistry is a simple schema registry that can be set by the user.
var defaultRegistry SchemaRegistry

// SetSchemaRegistry sets the default schema registry used for encoding/decoding.
func SetSchemaRegistry(r SchemaRegistry) {
	defaultRegistry = r
}

// DecodeData decodes Avro-encoded bytes into the target value.
// The target must have a registered schema in the schema registry,
// or implement the SchemaProvider interface.
func DecodeData(ctx context.Context, in []byte, out interface{}) error {
	schema, err := getSchemaFor(out)
	if err != nil {
		return fmt.Errorf("failed to get schema for decoding: %w", err)
	}

	if err := avro.Unmarshal(schema, in, out); err != nil {
		return fmt.Errorf("failed to unmarshal Avro data: %w", err)
	}
	return nil
}

// EncodeData encodes a value to Avro bytes.
// Like the official datacodec implementations, this one returns the given value
// as-is if it is already a byte slice.
func EncodeData(ctx context.Context, in interface{}) ([]byte, error) {
	if b, ok := in.([]byte); ok {
		return b, nil
	}

	schema, err := getSchemaFor(in)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for encoding: %w", err)
	}

	return avro.Marshal(schema, in)
}

// SchemaProvider is an interface that types can implement to provide their own Avro schema.
type SchemaProvider interface {
	AvroSchema() avro.Schema
}

// getSchemaFor retrieves the Avro schema for a given value.
func getSchemaFor(v interface{}) (avro.Schema, error) {
	// First check if the value implements SchemaProvider
	if sp, ok := v.(SchemaProvider); ok {
		return sp.AvroSchema(), nil
	}

	// Check pointer to value as well
	if sp, ok := interface{}(&v).(SchemaProvider); ok {
		return sp.AvroSchema(), nil
	}

	// Try the default registry
	if defaultRegistry != nil {
		return defaultRegistry.GetSchema(v)
	}

	return nil, fmt.Errorf("no schema available for type %T: implement SchemaProvider interface or set a SchemaRegistry", v)
}
