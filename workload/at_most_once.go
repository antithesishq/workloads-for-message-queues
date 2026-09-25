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
	atMostOnceTopic         = "at-most-once-plus-producer-progress-test"
	atMostOnceMessageCount  = 10
	atMostOnceLoopDelay     = 5 * time.Second
	producerProgressTimeout = 120 * time.Second
)

func runAtMostOnce(ctx context.Context) {
	createTopic(ctx, atMostOnceTopic, 1, 3, "2")

	producer := newClient("at-most-once",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(atMostOnceTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(1),
	)
	defer producer.Close()

	consumer := newClient("at-most-once",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(atMostOnceTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	defer consumer.Close()

	for i := range workloadIterations {
		log.Printf("[at-most-once] iteration %d/%d\n", i+1, workloadIterations)
		consumed := map[string]bool{}
		lastPublish := time.Now()

		for range atMostOnceMessageCount {
			assert.Always(
				time.Since(lastPublish) <= producerProgressTimeout,
				"Producer makes progress",
				map[string]any{"elapsed": time.Since(lastPublish).String(), "timeout": producerProgressTimeout.String()},
			)

			id := uuid.New().String()
			data := []byte(randomString(dataLength))

			opCtx, opCancel := context.WithTimeout(ctx, operationTimeout)
			err := producer.ProduceSync(opCtx, &kgo.Record{
				Key:   []byte(id),
				Value: data,
			}).FirstErr()
			opCancel()

			if err != nil {
				log.Printf("[at-most-once] publish failed uuid=%s: %v\n", id, err)
				continue
			}

			lastPublish = time.Now()
		}

		for {
			pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
			fetches := consumer.PollFetches(pollCtx)
			cancel()

			if fetches.NumRecords() == 0 {
				break
			}

			fetches.EachRecord(func(r *kgo.Record) {
				id := string(r.Key)

				assert.Always(!consumed[id], "At most once delivery", map[string]any{
					"uuid": id, "offset": r.Offset,
				})
				if consumed[id] {
					log.Printf("[at-most-once] duplicate uuid=%s offset=%d\n", id, r.Offset)
				}

				consumed[id] = true
			})
		}

		log.Printf("[at-most-once] consumed %d messages\n", len(consumed))
		log.Printf("[at-most-once] iteration done\n")
		time.Sleep(atMostOnceLoopDelay)
	}
}
