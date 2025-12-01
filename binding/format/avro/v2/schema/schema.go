/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package schema

import (
	_ "embed"

	"github.com/hamba/avro/v2"
)

//go:embed cloudevents.avsc
var cloudEventSchemaJSON string

// CloudEvent is the parsed Avro schema for CloudEvents.
var CloudEvent avro.Schema

func init() {
	var err error
	CloudEvent, err = avro.Parse(cloudEventSchemaJSON)
	if err != nil {
		panic("failed to parse CloudEvents Avro schema: " + err.Error())
	}
}

// CloudEventRecord represents the Avro record structure for CloudEvents.
// The Data field uses any to support the complex union type in the schema.
type CloudEventRecord struct {
	Attribute map[string]any `avro:"attribute"`
	Data      any            `avro:"data"`
}
