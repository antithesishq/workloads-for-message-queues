package main

import (
	"context"
	"log"
	"sync"
	"time"
	mathrand "math/rand/v2"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerProgressTopic       = "consumer-progress-test"
	consumerProgressCGroup      = "consumer-progress-cgroup" // required for producer's lag query to calculate backfill amount
	bufferMessageCount          = 100
	backfillInterval            = 1 * time.Second
	consumerProgressPollTimeout = 2 * time.Minute
)

func runConsumerProgress(ctx context.Context) {
	createTopic(ctx, consumerProgressTopic, 1, 3, "2")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ready := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		backfillProgress(ctx, ready)
	}()
	<-ready

	consumeProgress(ctx)
	cancel()
	wg.Wait()
}

func backfillProgress(ctx context.Context, ready chan<- struct{}) {
	client := newClient("consumer-progress",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(consumerProgressTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	defer client.Close()

	adm := kadm.NewClient(client)

	records := make([]*kgo.Record, bufferMessageCount)
	for i := range records {
		records[i] = &kgo.Record{
			Value: []byte(randomString(dataLength)),
		}
	}
	opCtx, opCancel := context.WithTimeout(ctx, operationTimeout)
	err := client.ProduceSync(opCtx, records...).FirstErr()
	opCancel()
	if err != nil {
		log.Printf("[consumer-progress] initial fill failed: %v\n", err)
	}
	log.Printf("[consumer-progress] initial fill of %d messages done\n", bufferMessageCount)
	close(ready)

	for ctx.Err() == nil {
		time.Sleep(backfillInterval)

		lagCtx, lagCancel := context.WithTimeout(ctx, operationTimeout)
		lags, err := adm.Lag(lagCtx, consumerProgressCGroup)
		lagCancel()
		if err != nil {
			log.Printf("[consumer-progress] lag query failed: %v\n", err)
			continue
		}

		topUp := bufferMessageCount - lags[consumerProgressCGroup].Lag.Total()
		if topUp <= 0 {
			continue
		}

		log.Printf("[consumer-progress] lag=%v topup=%v\n", lags[consumerProgressCGroup].Lag.Total(), topUp)

		records := make([]*kgo.Record, int(topUp))
		for i := range records {
			records[i] = &kgo.Record{
				Value: []byte(randomString(dataLength)),
			}
		}

		bfCtx, bfCancel := context.WithTimeout(ctx, operationTimeout)
		err = client.ProduceSync(bfCtx, records...).FirstErr()
		bfCancel()
		if err != nil {
			log.Printf("[consumer-progress] backfill failed: %v\n", err)
			continue
		}

		log.Printf("[consumer-progress] backfilled %d messages\n", topUp)
	}
}

func consumeProgress(ctx context.Context) {
	client := newClient("consumer-progress",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(consumerProgressTopic),
		kgo.ConsumerGroup(consumerProgressCGroup), // required for producer's lag query to calculate backfill amount
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer client.Close()

	adm := kadm.NewClient(client)
	lastConsumed := time.Now()

	for i := range workloadIterations {
		log.Printf("[consumer-progress] iteration %d/%d\n", i+1, workloadIterations)
		pollCtx, cancel := context.WithTimeout(ctx, consumerProgressPollTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.NumRecords() > 0 {
			lastConsumed = time.Now()
			log.Printf("[consumer-progress] consumed %d records\n", fetches.NumRecords())

			fetches.EachRecord(func(_ *kgo.Record) {
				time.Sleep(time.Duration(mathrand.IntN(400)+100) * time.Millisecond)
			})
		}

		elapsed := time.Since(lastConsumed)

		var lag int64
		cLagCtx, cLagCancel := context.WithTimeout(ctx, operationTimeout)
		if lags, err := adm.Lag(cLagCtx, consumerProgressCGroup); err == nil {
			lag = lags[consumerProgressCGroup].Lag.Total()
		}
		cLagCancel()

		stuck := elapsed > consumerProgressPollTimeout
		assert.Always(!stuck, "Consumer can make progress", map[string]any{
			"elapsed": elapsed.String(), "timeout": consumerProgressPollTimeout.String(), "lag": lag,
		})
		if stuck {
			log.Printf("[consumer-progress] consumer stuck, elapsed=%s lag=%d\n", elapsed, lag)
		}
	}
}
