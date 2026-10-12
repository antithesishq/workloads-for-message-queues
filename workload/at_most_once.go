package main

import (
	"context"
	"fmt"
	"io"
	"log"
	mathrand "math/rand/v2"
	"os"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	amoTopic             = "amo-test"
	amoConsumerGroup     = "amo-group"
	amoConsumerStateFile = "/tmp/amo-consumer-state.txt"
	amoSeenLineWidth     = 20 // "0000000000000000000\n"
)

func runAtMostOnce(ctx context.Context) {
	createTopic(ctx, amoTopic, 1, 3, "2")

	consumerState, err := os.OpenFile(amoConsumerStateFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("[amo] failed to open consumer state file: %v", err)
	}
	defer consumerState.Close()

	producer := newClient("amo-producer",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(amoTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(0),
	)
	defer producer.Close()

	consumer := newClient("amo-consumer",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(amoTopic),
		kgo.ConsumerGroup(amoConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	go amoProduce(ctx, producer)
	amoConsume(ctx, consumer, consumerState)
}

func amoProduce(ctx context.Context, client *kgo.Client) {
	for {
		uuid := mathrand.Int64()
		uuidStr := strconv.FormatInt(uuid, 10)
		data := []byte(randomString(dataLength))

		opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
		err := client.ProduceSync(opCtx, &kgo.Record{
			Key:   []byte(uuidStr),
			Value: data,
		}).FirstErr()
		cancel()

		if err != nil {
			log.Printf("[amo] publish failed uuid=%d: %v", uuid, err)
		} else {
			log.Printf("[amo] published uuid=%d", uuid)
		}
	}
}

func loadAmoSeenSet(f *os.File) map[int64]bool {
	seen := make(map[int64]bool)
	f.Seek(0, io.SeekStart)
	buf := make([]byte, amoSeenLineWidth)
	for {
		n, err := io.ReadFull(f, buf)
		if n < amoSeenLineWidth {
			break
		}
		uuid, _ := strconv.ParseInt(string(buf[0:19]), 10, 64)
		seen[uuid] = true
		if err != nil {
			break
		}
	}
	log.Printf("[amo] loaded consumer state, %d records", len(seen))
	return seen
}

func amoConsume(ctx context.Context, client *kgo.Client, stateFile *os.File) {
	seen := loadAmoSeenSet(stateFile)
	nextOffset, _ := stateFile.Seek(0, io.SeekEnd)

	for {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.NumRecords() == 0 {
			continue
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, operationTimeout)
		client.CommitUncommittedOffsets(commitCtx)
		commitCancel()

		fetches.EachRecord(func(r *kgo.Record) {
			uuid, err := strconv.ParseInt(string(r.Key), 10, 64)
			if err != nil {
				log.Printf("[amo] invalid message key: %q", string(r.Key))
				return
			}

			duplicate := seen[uuid]
			assert.Always(!duplicate, "At most once delivery", map[string]any{
				"uuid": uuid,
			})
			if duplicate {
				log.Printf("[amo] duplicate delivery uuid=%d", uuid)
			}

			seen[uuid] = true
			line := fmt.Sprintf("%019d\n", uuid)
			stateFile.WriteAt([]byte(line), nextOffset)
			stateFile.Sync()
			nextOffset += amoSeenLineWidth
		})
	}
}
