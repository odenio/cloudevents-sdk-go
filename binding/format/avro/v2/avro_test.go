/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package avro_test

import (
	"net/url"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"

	avrofmt "github.com/cloudevents/sdk-go/binding/format/avro/v2"
	"github.com/cloudevents/sdk-go/binding/format/avro/v2/schema"
)

func TestAvroFormatBasic(t *testing.T) {
	require := require.New(t)
	const test = "test"
	e := event.New()
	e.SetID(test)
	e.SetSource(test)
	e.SetType(test)
	require.NoError(e.SetData(event.ApplicationJSON, `{"foo":"bar"}`))

	b, err := avrofmt.Avro.Marshal(&e)
	require.NoError(err)
	require.NotEmpty(b)

	var e2 event.Event
	require.NoError(avrofmt.Avro.Unmarshal(b, &e2))

	require.Equal(e.ID(), e2.ID())
	require.Equal(e.Source(), e2.Source())
	require.Equal(e.Type(), e2.Type())
	require.Equal(e.SpecVersion(), e2.SpecVersion())
	require.Equal(e.DataContentType(), e2.DataContentType())
	require.Equal(e.Data(), e2.Data())
}

func TestAvroFormatWithAllAttributes(t *testing.T) {
	require := require.New(t)
	const test = "test"
	e := event.New()
	e.SetID(test)
	e.SetTime(stdtime.Date(2021, 1, 1, 1, 1, 1, 1, stdtime.UTC))
	e.SetExtension(test, test)
	e.SetExtension("int", 1)
	e.SetExtension("bool", true)
	e.SetExtension("URI", &url.URL{
		Scheme: "https",
		Host:   "test-uri.com",
	})
	e.SetExtension("URIRef", types.URIRef{URL: url.URL{
		Scheme: "https",
		Host:   "test-uriref.com",
	}})
	e.SetExtension("bytes", []byte(test))
	e.SetExtension("timestamp", stdtime.Date(2021, 2, 1, 1, 1, 1, 1, stdtime.UTC))
	e.SetSubject(test)
	e.SetSource(test)
	e.SetType(test)
	e.SetDataSchema(test)
	require.NoError(e.SetData(event.ApplicationJSON, "foo"))

	b, err := avrofmt.Avro.Marshal(&e)
	require.NoError(err)
	require.NotEmpty(b)

	var e2 event.Event
	require.NoError(avrofmt.Avro.Unmarshal(b, &e2))

	// Check required attributes
	require.Equal(e.ID(), e2.ID())
	require.Equal(e.Source(), e2.Source())
	require.Equal(e.Type(), e2.Type())
	require.Equal(e.SpecVersion(), e2.SpecVersion())

	// Check optional attributes
	require.Equal(e.Subject(), e2.Subject())
	require.Equal(e.DataSchema(), e2.DataSchema())
	require.Equal(e.DataContentType(), e2.DataContentType())
	require.True(e.Time().Equal(e2.Time()), "times should be equal: %v vs %v", e.Time(), e2.Time())

	// Check data
	require.Equal(e.Data(), e2.Data())

	// Check extension attributes
	// Note: CloudEvents SDK normalizes extension names to lowercase
	ext2 := e2.Extensions()
	require.Equal(test, ext2[test])
	require.Equal(int32(1), ext2["int"])
	require.Equal(true, ext2["bool"])
	require.Equal([]byte(test), ext2["bytes"])

	// URI and URIRef are converted to strings in Avro format
	// Extension names are lowercased by the SDK
	require.Equal("https://test-uri.com", ext2["uri"])
	require.Equal("https://test-uriref.com", ext2["uriref"])

	// Timestamp extensions are stored as RFC3339 strings
	require.Equal("2021-02-01T01:01:01.000000001Z", ext2["timestamp"])
}

func TestAvroFormatWithBinaryData(t *testing.T) {
	require := require.New(t)
	e := event.New()
	e.SetID("binary-test")
	e.SetSource("test-source")
	e.SetType("test.type")
	e.SetDataContentType("application/octet-stream")
	e.DataEncoded = []byte{0x00, 0x01, 0x02, 0x03, 0xFF}

	b, err := avrofmt.Avro.Marshal(&e)
	require.NoError(err)

	var e2 event.Event
	require.NoError(avrofmt.Avro.Unmarshal(b, &e2))

	require.Equal(e.Data(), e2.Data())
}

func TestAvroFormatWithNoData(t *testing.T) {
	require := require.New(t)
	e := event.New()
	e.SetID("no-data-test")
	e.SetSource("test-source")
	e.SetType("test.type")

	b, err := avrofmt.Avro.Marshal(&e)
	require.NoError(err)

	var e2 event.Event
	require.NoError(avrofmt.Avro.Unmarshal(b, &e2))

	require.Equal(e.ID(), e2.ID())
	require.Equal(e.Source(), e2.Source())
	require.Equal(e.Type(), e2.Type())
	require.Nil(e2.Data())
}

func TestAvroFormatMediaType(t *testing.T) {
	require.Equal(t, "application/cloudevents+avro", avrofmt.Avro.MediaType())
	require.Equal(t, "application/cloudevents+avro", avrofmt.ApplicationCloudEventsAvro)
}

func TestToAvroAndFromAvro(t *testing.T) {
	require := require.New(t)
	e := event.New()
	e.SetID("roundtrip-test")
	e.SetSource("test-source")
	e.SetType("test.type")
	e.SetSubject("test-subject")
	e.SetTime(stdtime.Date(2024, 1, 15, 10, 30, 0, 0, stdtime.UTC))
	require.NoError(e.SetData(event.ApplicationJSON, map[string]string{"key": "value"}))

	// Convert to Avro record
	record, err := avrofmt.ToAvro(&e)
	require.NoError(err)
	require.NotNil(record)

	// Verify record contents
	require.Equal("1.0", record.Attribute["specversion"])
	require.Equal("roundtrip-test", record.Attribute["id"])
	require.Equal("test-source", record.Attribute["source"])
	require.Equal("test.type", record.Attribute["type"])
	require.Equal("test-subject", record.Attribute["subject"])
	require.NotNil(record.Data)

	// Convert back to event
	e2, err := avrofmt.FromAvro(record)
	require.NoError(err)

	require.Equal(e.ID(), e2.ID())
	require.Equal(e.Source(), e2.Source())
	require.Equal(e.Type(), e2.Type())
	require.Equal(e.Subject(), e2.Subject())
	require.True(e.Time().Equal(e2.Time()))
}

func TestStringOfApplicationCloudEventsAvro(t *testing.T) {
	ptr := avrofmt.StringOfApplicationCloudEventsAvro()
	require.NotNil(t, ptr)
	require.Equal(t, "application/cloudevents+avro", *ptr)
}

func TestSchemaPackage(t *testing.T) {
	require := require.New(t)

	// Verify schema is properly loaded
	require.NotNil(schema.CloudEvent)

	// Verify CloudEventRecord works
	record := &schema.CloudEventRecord{
		Attribute: map[string]any{
			"specversion": "1.0",
			"id":          "test-id",
			"source":      "test-source",
			"type":        "test.type",
		},
		Data: []byte("test data"),
	}
	require.Equal("1.0", record.Attribute["specversion"])
	require.Equal([]byte("test data"), record.Data)
}
