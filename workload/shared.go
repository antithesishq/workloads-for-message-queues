package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	mathrand "math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	dataLength         = 6400
	pollTimeout        = 10 * time.Second
	operationTimeout   = 30 * time.Second
	workloadIterations = 20
)

var brokers = []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}

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

const maxClientRetries = 10

func newClient(name string, opts ...kgo.Opt) *kgo.Client {
	for attempt := range maxClientRetries {
		client, err := kgo.NewClient(opts...)
		if err == nil {
			return client
		}
		log.Printf("[%s] failed to create client (attempt %d/%d): %v\n", name, attempt+1, maxClientRetries, err)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("[%s] failed to create client after %d attempts", name, maxClientRetries)
	return nil
}

func createTopic(ctx context.Context, topicName string, partitions int32, replicationFactor int16, isr string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatal("failed to dial for topic setup:", err)
	}
	defer client.Close()

	configs := map[string]*string{
		"min.insync.replicas": kadm.StringPtr(isr),
	}

	_, err = kadm.NewClient(client).CreateTopics(ctx, partitions, replicationFactor, configs, topicName)
	if err != nil {
		log.Printf("[createTopic] %v (continuing; topic may already exist)\n", err)
	}
}
