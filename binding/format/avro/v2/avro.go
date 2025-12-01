/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package avro

import (
	"encoding/json"
	"fmt"
	stdtime "time"

	"github.com/hamba/avro/v2"

	"github.com/cloudevents/sdk-go/binding/format/avro/v2/schema"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"
)

const (
	// Attribute keys for CloudEvents attributes stored in the attribute map
	datacontenttype = "datacontenttype"
	dataschema      = "dataschema"
	subject         = "subject"
	time            = "time"
	specversion     = "specversion"
	id              = "id"
	source          = "source"
	typ             = "type"
)

var zeroTime = stdtime.Time{}

// Avro is the built-in "application/cloudevents+avro" format.
var Avro = avroFmt{}

const (
	// ApplicationCloudEventsAvro is the content type for CloudEvents in Avro format.
	ApplicationCloudEventsAvro = "application/cloudevents+avro"
)

func init() {
	format.Add(Avro)
}

// StringOfApplicationCloudEventsAvro returns a string pointer to
// "application/cloudevents+avro"
func StringOfApplicationCloudEventsAvro() *string {
	a := ApplicationCloudEventsAvro
	return &a
}

type avroFmt struct{}

func (avroFmt) MediaType() string {
	return ApplicationCloudEventsAvro
}

func (avroFmt) Marshal(e *event.Event) ([]byte, error) {
	record, err := ToAvro(e)
	if err != nil {
		return nil, err
	}
	return avro.Marshal(schema.CloudEvent, record)
}

func (avroFmt) Unmarshal(b []byte, e *event.Event) error {
	record := &schema.CloudEventRecord{}
	if err := avro.Unmarshal(schema.CloudEvent, b, record); err != nil {
		return err
	}
	e2, err := FromAvro(record)
	if err != nil {
		return err
	}
	*e = *e2
	return nil
}

// ToAvro converts an SDK event to an Avro record that can be marshaled.
func ToAvro(e *event.Event) (*schema.CloudEventRecord, error) {
	record := &schema.CloudEventRecord{
		Attribute: make(map[string]any),
	}

	// Required attributes
	record.Attribute[specversion] = e.SpecVersion()
	record.Attribute[id] = e.ID()
	record.Attribute[source] = e.Source()
	record.Attribute[typ] = e.Type()

	// Optional attributes
	if e.DataContentType() != "" {
		record.Attribute[datacontenttype] = e.DataContentType()
	}
	if e.DataSchema() != "" {
		record.Attribute[dataschema] = e.DataSchema()
	}
	if e.Subject() != "" {
		record.Attribute[subject] = e.Subject()
	}
	if e.Time() != zeroTime {
		// Timestamps are encoded as RFC 3339 strings per the spec
		record.Attribute[time] = e.Time().Format(stdtime.RFC3339Nano)
	}

	// Extension attributes
	for name, value := range e.Extensions() {
		attrValue, err := attributeValueFor(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode extension attribute %s: %w", name, err)
		}
		record.Attribute[name] = attrValue
	}

	// Data - stored as bytes
	if data := e.Data(); data != nil {
		record.Data = data
	}

	return record, nil
}

// attributeValueFor converts a Go value to an Avro-compatible attribute value.
// Per the spec, attributes can be: null, boolean, int, string, or bytes
func attributeValueFor(v interface{}) (any, error) {
	vv, err := types.Validate(v)
	if err != nil {
		return nil, err
	}

	switch vt := vv.(type) {
	case bool:
		return vt, nil
	case int32:
		return int(vt), nil
	case string:
		return vt, nil
	case []byte:
		return vt, nil
	case types.URI:
		// URIs are encoded as strings per the spec
		return vt.String(), nil
	case types.URIRef:
		// URI-references are encoded as strings per the spec
		return vt.String(), nil
	case types.Timestamp:
		// Timestamps are encoded as RFC 3339 strings per the spec
		return vt.Time.Format(stdtime.RFC3339Nano), nil
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", v)
	}
}

// FromAvro converts an Avro record back into the generic SDK event.
func FromAvro(record *schema.CloudEventRecord) (*event.Event, error) {
	e := event.New()

	// Extract required attributes
	if v, ok := record.Attribute[specversion]; ok {
		if sv, ok := v.(string); ok {
			e.SetSpecVersion(sv)
		}
	}
	if v, ok := record.Attribute[id]; ok {
		if sv, ok := v.(string); ok {
			e.SetID(sv)
		}
	}
	if v, ok := record.Attribute[source]; ok {
		if sv, ok := v.(string); ok {
			e.SetSource(sv)
		}
	}
	if v, ok := record.Attribute[typ]; ok {
		if sv, ok := v.(string); ok {
			e.SetType(sv)
		}
	}

	// Extract optional and extension attributes
	for name, value := range record.Attribute {
		// Skip required attributes already handled
		if name == specversion || name == id || name == source || name == typ {
			continue
		}

		switch name {
		case datacontenttype:
			if sv, ok := value.(string); ok {
				e.SetDataContentType(sv)
			}
		case dataschema:
			if sv, ok := value.(string); ok {
				e.SetDataSchema(sv)
			}
		case subject:
			if sv, ok := value.(string); ok {
				e.SetSubject(sv)
			}
		case time:
			if sv, ok := value.(string); ok {
				t, err := stdtime.Parse(stdtime.RFC3339Nano, sv)
				if err != nil {
					// Try without nano precision
					t, err = stdtime.Parse(stdtime.RFC3339, sv)
					if err != nil {
						return nil, fmt.Errorf("failed to parse time attribute: %w", err)
					}
				}
				e.SetTime(t)
			}
		default:
			// Extension attribute
			extValue, err := extensionValueFrom(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert extension %s: %w", name, err)
			}
			e.SetExtension(name, extValue)
		}
	}

	// Set data - handle various types from the union
	// hamba/avro wraps union values in a map with the type name as key
	if record.Data != nil {
		switch d := record.Data.(type) {
		case []byte:
			e.DataEncoded = d
		case string:
			e.DataEncoded = []byte(d)
		case map[string]any:
			// Check if this is a wrapped union value from hamba/avro
			if bytes, ok := d["bytes"].([]byte); ok {
				e.DataEncoded = bytes
			} else if str, ok := d["string"].(string); ok {
				e.DataEncoded = []byte(str)
			} else {
				// JSON-like data structure, encode as JSON bytes
				jsonBytes, err := json.Marshal(d)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal map data: %w", err)
				}
				e.DataEncoded = jsonBytes
			}
		}
	}

	return &e, nil
}

// extensionValueFrom converts an Avro attribute value back to a Go value.
func extensionValueFrom(v any) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch vt := v.(type) {
	case bool:
		return vt, nil
	case int:
		return int32(vt), nil
	case int32:
		return vt, nil
	case int64:
		return int32(vt), nil
	case string:
		return vt, nil
	case []byte:
		return vt, nil
	default:
		return nil, fmt.Errorf("unsupported attribute type: %T", v)
	}
}
