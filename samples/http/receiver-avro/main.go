/*
 Copyright 2024 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"log"

	_ "github.com/cloudevents/sdk-go/binding/format/avro/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

func main() {
	ctx := context.Background()
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Printf("will listen on :8080\n")
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, receive))
}

func receive(ctx context.Context, event cloudevents.Event) {
	log.Printf("%s", event)
	payload := &Sample{}
	if err := event.DataAs(payload); err != nil {
		log.Printf("failed to decode avro data: %s", err)
		return
	}
	log.Printf("decoded avro: %s", payload.Value)
}
