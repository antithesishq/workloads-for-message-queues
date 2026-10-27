package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"os"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumer struct {
	client   *kgo.Client
	stateLog *os.File
	checker  *checker
}

func newConsumerClient(brokers []string, from int64) *kgo.Client {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(from)},
		}),
	)
	if err != nil {
		log.Fatalf("new consumer client: %v", err)
	}
	return cl
}

func (c *consumer) run() {
	for {
		fetches := c.client.PollFetches(context.Background())
		// fetches.EachError(func(_ string, _ int32, err error) {
		// 	var dataLoss *kgo.ErrDataLoss
		// 	if errors.As(err, &dataLoss) {
		// 		// Partition reset to a newer offset: records between the two were lost.
		// 		assert.Unreachable("No data loss: the partition never loses records the consumer already read",
		// 			map[string]any{
		// 				"consumed_to": dataLoss.ConsumedTo,
		// 				"reset_to":    dataLoss.ResetTo,
		// 			})
		// 	}
		// 	log.Printf("fetch error: %v", err)
		// })
		fetches.EachRecord(c.process)
	}
}

func (c *consumer) process(r *kgo.Record) {
	uuid := string(r.Key)

	var expected string
	for _, h := range r.Headers {
		if h.Key == "checksum" {
			expected = string(h.Value)
			break
		}
	}
	sum := sha256.Sum256(r.Value)
	computed := hex.EncodeToString(sum[:])

	assert.Always(expected == computed,
		"No data corruption: every consumed message matches its checksum",
		map[string]any{
			"uuid":            uuid,
			"offset":          r.Offset,
			"expected": 	   expected,
			"computed":		   computed,
		})

	c.checker.checkConsumed(uuid, r.Offset)
	writeLine(c.stateLog, "%s, %d", uuid, r.Offset)
}
