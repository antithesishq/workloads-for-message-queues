package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	safetyTopic        = "safety-test"
	safetyMessageCount = 10
	safetyLoopDelay    = 60 * time.Second
)

func runSafety(ctx context.Context) {
	createTopic(ctx, safetyTopic, 1, 3, "2")

	producer := newClient("safety",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(safetyTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	defer producer.Close()

	consumer := newClient("safety",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(safetyTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	defer consumer.Close()

	for range workloadIterations {
		acked := map[string]bool{}
		consumed := map[string]bool{}

		for range safetyMessageCount {
			id := uuid.New().String()
			data := []byte(randomString(dataLength))
			checksum := sha256sum(data)

			err := producer.ProduceSync(ctx, &kgo.Record{
				Key:   []byte(id),
				Value: data,
				Headers: []kgo.RecordHeader{
					{Key: "checksum", Value: []byte(checksum)},
				},
			}).FirstErr()

			if err != nil {
				log.Printf("[safety] publish failed uuid=%s: %v\n", id, err)
				continue
			}

			acked[id] = true
		}

		log.Printf("[safety] produced %d messages\n", len(acked))

		for {
			pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
			fetches := consumer.PollFetches(pollCtx)
			cancel()

			if fetches.NumRecords() == 0 {
				break
			}

			fetches.EachRecord(func(r *kgo.Record) {
				id := string(r.Key)

				assert.Always(acked[id], "No phantom messages", map[string]any{
					"uuid": id, "offset": r.Offset,
				})
				if !acked[id] {
					log.Printf("[safety] phantom message uuid=%s offset=%d\n", id, r.Offset)
				}

				var expected string
				for _, h := range r.Headers {
					if h.Key == "checksum" {
						expected = string(h.Value)
						break
					}
				}
				computed := sha256sum(r.Value)
				assert.Always(computed == expected, "No data corruption", map[string]any{
					"uuid": id, "expected": expected, "computed": computed, "offset": r.Offset,
				})

				consumed[id] = true
			})
		}

		for id := range acked {
			assert.Always(consumed[id], "No data loss", map[string]any{"uuid": id})
			if !consumed[id] {
				log.Printf("[safety] data loss uuid=%s\n", id)
			}
		}

		log.Printf("[safety] consumed %d messages\n", len(consumed))
		log.Printf("[safety] iteration done, acked=%d consumed=%d\n", len(acked), len(consumed))
		time.Sleep(safetyLoopDelay)
	}
}
