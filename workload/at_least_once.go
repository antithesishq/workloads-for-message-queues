package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	atLeastOnceTopic         = "at-least-once-plus-ordering-test"
	numConsumerGroups        = 3
	atLeastOnceMessageCount  = 10
	atLeastOnceLoopDelay     = 5 * time.Second
)

type consumerState struct {
	client      *kgo.Client
	group       string
	expectedSeq int64
}

func runAtLeastOnce(ctx context.Context) {
	createTopic(ctx, atLeastOnceTopic, 1, 3, "2")

	producer := newClient("at-least-once",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(atLeastOnceTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	defer producer.Close()

	consumers := make([]consumerState, numConsumerGroups)
	for i := range consumers {
		group := fmt.Sprintf("at-least-once-group-%d", i)
		consumers[i] = consumerState{
			client: newClient("at-least-once",
				kgo.SeedBrokers(brokers...),
				kgo.ConsumeTopics(atLeastOnceTopic),
				kgo.ConsumerGroup(group),
				kgo.DisableAutoCommit(),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			),
			group:       group,
			expectedSeq: 1,
		}
	}
	defer func() {
		for _, c := range consumers {
			c.client.Close()
		}
	}()

	var seq int64

	for i := range workloadIterations {
		log.Printf("[at-least-once] iteration %d/%d\n", i+1, workloadIterations)
		acked := map[string]bool{}

		for range atLeastOnceMessageCount {
			seq++
			id := uuid.New().String()
			data := []byte(randomString(dataLength))

			opCtx, opCancel := context.WithTimeout(ctx, operationTimeout)
			err := producer.ProduceSync(opCtx, &kgo.Record{
				Key:   []byte(id),
				Value: data,
				Headers: []kgo.RecordHeader{
					{Key: "seq", Value: []byte(strconv.FormatInt(seq, 10))},
				},
			}).FirstErr()
			opCancel()

			if err != nil {
				log.Printf("[at-least-once] publish failed uuid=%s seq=%d: %v\n", id, seq, err)
				continue
			}

			acked[id] = true
		}

		log.Printf("[at-least-once] produced %d messages\n", len(acked))

		var wg sync.WaitGroup
		for i := range consumers {
			wg.Add(1)
			go func(cs *consumerState) {
				defer wg.Done()
				consumed := map[string]bool{}

				for {
					pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
					fetches := cs.client.PollFetches(pollCtx)
					cancel()

					if fetches.NumRecords() == 0 {
						break
					}

					fetches.EachRecord(func(r *kgo.Record) {
						id := string(r.Key)
						msgSeq := parseSeqHeader(r.Headers)

						if msgSeq < cs.expectedSeq {
							// Duplicate, expected for at-least-once
						} else {
							inOrder := msgSeq == cs.expectedSeq
							assert.Always(inOrder, "Message ordering", map[string]any{
								"group": cs.group, "expected": cs.expectedSeq, "got": msgSeq, "uuid": id,
							})
							if !inOrder {
								log.Printf("[at-least-once] ordering violation group=%s expected=%d got=%d uuid=%s\n",
									cs.group, cs.expectedSeq, msgSeq, id)
							}
							cs.expectedSeq = msgSeq + 1
						}

						consumed[id] = true
					})
				}

				commitCtx, commitCancel := context.WithTimeout(ctx, operationTimeout)
				cs.client.CommitUncommittedOffsets(commitCtx)
				commitCancel()

				for id := range acked {
					assert.Always(consumed[id], "At least once delivery", map[string]any{
						"group": cs.group, "uuid": id,
					})
					if !consumed[id] {
						log.Printf("[at-least-once] missing message group=%s uuid=%s\n", cs.group, id)
					}
				}
			}(&consumers[i])
		}
		wg.Wait()

		log.Printf("[at-least-once] consumed across %d groups\n", numConsumerGroups)
		log.Printf("[at-least-once] iteration done\n")
		time.Sleep(atLeastOnceLoopDelay)
	}
}

func parseSeqHeader(headers []kgo.RecordHeader) int64 {
	for _, h := range headers {
		if h.Key == "seq" {
			n, _ := strconv.ParseInt(string(h.Value), 10, 64)
			return n
		}
	}
	return 0
}
