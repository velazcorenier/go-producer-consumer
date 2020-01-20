package main

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
	// Instantiate a Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		log.Fatal(err)
	}

	// Use the client object to instantiate a consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-golang-topic",
		SubscriptionName: "sub-1",
		Type:             pulsar.Exclusive,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	ctx := context.Background()

	// Listen indefinitely on the topic
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}

		// Do something with the message
		fmt.Printf("Message: %s", string(msg.Payload()))

		if err == nil {
			// Message processed successfully
			consumer.Ack(msg)
		} else {
			// Failed to process messages
			consumer.Nack(msg)
		}
	}
}
