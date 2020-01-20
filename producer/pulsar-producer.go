package main

import (
	"context"
	"log"
	"runtime"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     "pulsar://localhost:6650",
		OperationTimeoutSeconds: 5,
		MessageListenerThreads:  runtime.NumCPU(),
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-golang-topic",
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar producer: %v", err)
	}

	defer producer.Close()

	for i := 0; i < 10; i++ {
		msg := pulsar.ProducerMessage{
			Payload: []byte("Hello, Pulsar\n"),
		}

		if err := producer.Send(context.Background(), msg); err != nil {
			log.Fatalf("Producer could not send message: %v", err)
		}
	}

}
