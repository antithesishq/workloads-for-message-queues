package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	mathrand "math/rand/v2"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokers = []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}

const (
	topic        = "no-data-corruption-and-liveness-test"
	messageCount = 10
	dataLength   = 64
	pollTimeout  = 5 * time.Second
)

var checksums = map[string]string{}

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[mathrand.IntN(len(alphabet))]
	}
	return string(b)
}

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

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
		log.Printf("createTopic: %v (continuing; topic may already exist)\n", err)
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

	for range messageCount {
		id := uuid.New().String()
		data := []byte(randomString(dataLength))

		err := client.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(id),
			Value: data,
		}).FirstErr()

		if err != nil {
			log.Printf("publish failed uuid=%s: %v\n", id, err)
			continue
		}

		checksums[id] = sha256sum(data)
		log.Printf("published uuid=%s\n", id)
	}
	log.Printf("producer done: %d messages recorded in hashtable\n", len(checksums))
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
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.NumRecords() == 0 {
			log.Println("consumer done: no new messages within poll timeout, queue assumed empty")
			return
		}

		log.Printf("consumer done: %d records fetched.\n", fetches.NumRecords())

		fetches.EachRecord(verifyChecksum)
	}
}

func verifyChecksum(r *kgo.Record) {
	id := string(r.Key)
	computed := sha256sum(r.Value)
	expected, ok := checksums[id]
	if !ok {
		log.Printf("untracked uuid=%s offset=%d (likely from prior run)\n", id, r.Offset)
		return
	}
	assert.Always(
		computed == expected,
		"consumed message checksum matches produced (no data corruption)",
		map[string]any{
			"uuid":     id,
			"expected": expected,
			"computed": computed,
			"offset":   r.Offset,
		},
	)
	log.Printf("verified uuid=%s offset=%d\n", id, r.Offset)
}
