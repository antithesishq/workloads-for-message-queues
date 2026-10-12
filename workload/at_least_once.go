package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	mathrand "math/rand/v2"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	aloTopic         = "alo-test"
	aloConsumerGroup = "alo-group"
	aloProducerLog   = "/tmp/alo-producer.log"
	aloConsumerLog   = "/tmp/alo-consumer.log"
)

func runAtLeastOnce(ctx context.Context) {
	createTopic(ctx, aloTopic, 1, 3, "2")

	producerLog, err := os.OpenFile(aloProducerLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("[alo] failed to open producer log: %v", err)
	}
	defer producerLog.Close()

	consumerLog, err := os.OpenFile(aloConsumerLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("[alo] failed to open consumer log: %v", err)
	}
	defer consumerLog.Close()

	producerLogReader, err := os.Open(aloProducerLog)
	if err != nil {
		log.Fatalf("[alo] failed to open producer log for reading: %v", err)
	}
	defer producerLogReader.Close()

	consumerLogReader, err := os.Open(aloConsumerLog)
	if err != nil {
		log.Fatalf("[alo] failed to open consumer log for reading: %v", err)
	}
	defer consumerLogReader.Close()

	producer := newClient("alo-producer",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(aloTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(0),
	)
	defer producer.Close()

	consumer := newClient("alo-consumer",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(aloTopic),
		kgo.ConsumerGroup(aloConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	go aloProduce(ctx, producer, producerLog)
	go aloConsume(ctx, consumer, consumerLog)
	aloOracle(producerLogReader, consumerLogReader)
}

func aloProduce(ctx context.Context, client *kgo.Client, logFile *os.File) {
	for {
		uuid := mathrand.Int64()
		uuidStr := strconv.FormatInt(uuid, 10)
		data := []byte(randomString(dataLength))

		for {
			opCtx, cancel := context.WithTimeout(ctx, operationTimeout)
			err := client.ProduceSync(opCtx, &kgo.Record{
				Key:   []byte(uuidStr),
				Value: data,
			}).FirstErr()
			cancel()

			if err == nil {
				fmt.Fprintf(logFile, "%d,1\n", uuid)
				logFile.Sync()
				log.Printf("[alo] published uuid=%d", uuid)
				break
			}

			fmt.Fprintf(logFile, "%d,0\n", uuid)
			logFile.Sync()
			log.Printf("[alo] produce failed uuid=%d: %v", uuid, err)
			time.Sleep(1 * time.Second)
		}
	}
}

func aloConsume(ctx context.Context, client *kgo.Client, logFile *os.File) {
	for {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollFetches(pollCtx)
		cancel()

		if fetches.NumRecords() == 0 {
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			uuid, err := strconv.ParseInt(string(r.Key), 10, 64)
			if err != nil {
				log.Printf("[alo] invalid message key: %q", string(r.Key))
				return
			}
			fmt.Fprintf(logFile, "%d\n", uuid)
			logFile.Sync()
		})

		commitCtx, commitCancel := context.WithTimeout(ctx, operationTimeout)
		client.CommitUncommittedOffsets(commitCtx)
		commitCancel()
	}
}

func aloOracle(producerLog, consumerLog *os.File) {
	type uuidState struct {
		attempts int
		acks     int
		consumed int
	}
	state := make(map[int64]*uuidState)

	producerReader := bufio.NewReader(producerLog)
	consumerReader := bufio.NewReader(consumerLog)
	var producerPartial, consumerPartial string

	for {
		producerPartial = drainLines(producerReader, producerPartial, func(line string) {
			parts := strings.SplitN(line, ",", 2)
			if len(parts) != 2 {
				return
			}
			uuid, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return
			}
			ack, _ := strconv.Atoi(parts[1])

			s, ok := state[uuid]
			if !ok {
				s = &uuidState{}
				state[uuid] = s
			}
			s.attempts++
			if ack == 1 {
				s.acks++
			}
		})

		consumerPartial = drainLines(consumerReader, consumerPartial, func(line string) {
			uuid, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return
			}

			s, ok := state[uuid]
			if !ok {
				s = &uuidState{}
				state[uuid] = s
			}
			s.consumed++

			if s.attempts > 0 {
				inBounds := s.consumed >= s.acks && s.consumed <= s.attempts
				assert.Always(inBounds, "At least once delivery", map[string]any{
					"uuid": uuid, "consumed": s.consumed, "attempts": s.attempts, "acks": s.acks,
				})
				if !inBounds {
					log.Printf("[alo] bounds violation uuid=%d consumed=%d acks=%d attempts=%d",
						uuid, s.consumed, s.acks, s.attempts)
				}
			}
		})

		time.Sleep(100 * time.Millisecond)
	}
}

func drainLines(reader *bufio.Reader, partial string, handler func(string)) string {
	for {
		chunk, err := reader.ReadString('\n')
		partial += chunk

		if err == nil {
			handler(strings.TrimRight(partial, "\n"))
			partial = ""
			continue
		}

		if err == io.EOF {
			return partial
		}

		log.Printf("[alo] log read error: %v", err)
		return partial
	}
}
