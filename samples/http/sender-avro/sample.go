/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	_ "embed"

	"github.com/hamba/avro/v2"
)

//go:embed sample.avsc
var sampleSchemaJSON string

var sampleSchema avro.Schema

func init() {
	var err error
	sampleSchema, err = avro.Parse(sampleSchemaJSON)
	if err != nil {
		panic("failed to parse Sample Avro schema: " + err.Error())
	}
}

// Sample is a sample Avro record.
type Sample struct {
	Value string `avro:"value"`
}

// AvroSchema implements the avro.SchemaProvider interface.
func (s *Sample) AvroSchema() avro.Schema {
	return sampleSchema
}
