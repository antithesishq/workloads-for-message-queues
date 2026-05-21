package main

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokers = []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}

const topic = "no-data-corruption-and-liveness-test"

func createTopic(ctx context.Context) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatal("failed to dial for topic setup:", err)
	}
	defer client.Close()

	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr("2"),
	}

	_, err = kadm.NewClient(client).CreateTopics(ctx, 1, 3, configs, topic)
	if err != nil {
		log.Printf("createTopic: %v (continuing; topic may already exist)", err)
	}
}

func produce(ctx context.Context) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		log.Fatal("failed to create producer:", err)
	}
	defer client.Close()

	records := []*kgo.Record{
		{Key: []byte("Key-A"), Value: []byte("Hello World!")},
		{Key: []byte("Key-B"), Value: []byte("One!")},
		{Key: []byte("Key-C"), Value: []byte("Two!")},
	}

	results := client.ProduceSync(ctx, records...)
	for _, r := range results {
		if r.Err != nil {
			log.Fatal("failed to write message:", r.Err)
		}
	}
}

func consume(ctx context.Context) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal("failed to create consumer:", err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch err topic=%s partition=%d: %v", e.Topic, e.Partition, e.Err)
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			log.Printf("message at offset %d: %s = %s", r.Offset, r.Key, r.Value)
		})
	}
}
