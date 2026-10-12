package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	safetyTopic             = "safety-test"
	safetyStateFile         = "/tmp/safety-state.txt"
	safetyConsumerStateFile = "/tmp/safety-consumer-state.txt"
	safetyConsumerGroup     = "safety-group"
)

func runSafety(ctx context.Context) {
	createTopic(ctx, safetyTopic, 1, 3, "2")

	stateFile, err := os.OpenFile(safetyStateFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("[safety] failed to open state file: %v", err)
	}
	defer stateFile.Close()

	var startUUID int64
	scanner := bufio.NewScanner(stateFile)
	for scanner.Scan() {
		startUUID++
	}
	stateFile.Seek(0, io.SeekEnd)

	lastConsumed := readConsumerState()

	readFile, err := os.Open(safetyStateFile)
	if err != nil {
		log.Fatalf("[safety] failed to open state file for reading: %v", err)
	}
	defer readFile.Close()

	reader := bufio.NewReader(readFile)
	for i := int64(0); i < lastConsumed; i++ {
		if _, err := reader.ReadString('\n'); err != nil {
			log.Printf("[safety] could not skip to line %d, resetting to 0", lastConsumed)
			readFile.Seek(0, io.SeekStart)
			reader.Reset(readFile)
			lastConsumed = 0
			break
		}
	}
	log.Printf("[safety] resuming consumer at lastConsumed=%d", lastConsumed)

	consumer := newClient("safety-consumer",
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(safetyTopic),
		kgo.ConsumerGroup(safetyConsumerGroup),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	producer := newClient("safety-producer",
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(safetyTopic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	defer producer.Close()

	go safetyProduce(ctx, producer, stateFile, startUUID)
	safetyConsume(ctx, consumer, reader, lastConsumed)
}

func safetyProduce(ctx context.Context, client *kgo.Client, stateFile *os.File, startUUID int64) {
	uuid := startUUID

	for {
		uuid++
		uuidStr := strconv.FormatInt(uuid, 10)
		data := []byte(randomString(dataLength))
		checksum := sha256sum(data)

		line := uuidStr + ",0\n"
		pos, _ := stateFile.Seek(0, io.SeekEnd)
		ackOffset := pos + int64(len(uuidStr)) + 1
		stateFile.WriteString(line)
		stateFile.Sync()

		opCtx, opCancel := context.WithTimeout(ctx, operationTimeout)
		err := client.ProduceSync(opCtx, &kgo.Record{
			Key:   []byte(uuidStr),
			Value: data,
			Headers: []kgo.RecordHeader{
				{Key: "checksum", Value: []byte(checksum)},
			},
		}).FirstErr()
		opCancel()

		if err != nil {
			log.Printf("[safety] publish failed uuid=%d: %v", uuid, err)
		} else {
			stateFile.WriteAt([]byte("1"), ackOffset)
			stateFile.Sync()
			log.Printf("[safety] published uuid=%d", uuid)
		}
	}
}

func safetyConsume(ctx context.Context, client *kgo.Client, reader *bufio.Reader, lastConsumed int64) {
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
				log.Printf("[safety] invalid message key: %q", string(r.Key))
				return
			}

			// Redelivery of last message is cool
			if uuid == lastConsumed {
				return
			}

			prevConsumed := lastConsumed

			// 1. No data corruption
			var checksum string
			for _, h := range r.Headers {
				if h.Key == "checksum" {
					checksum = string(h.Value)
					break
				}
			}
			computed := sha256sum(r.Value)
			assert.Always(computed == checksum, "No data corruption", map[string]any{
				"uuid": uuid, "expected": checksum, "computed": computed,
			})
			if computed != checksum {
				log.Printf("[safety] data corruption uuid=%d", uuid)
			}

			// 2. Message ordering
			ordered := uuid > prevConsumed
			assert.Always(ordered, "Message ordering", map[string]any{
				"uuid": uuid, "prevConsumed": prevConsumed,
			})
			if !ordered {
				log.Printf("[safety] ordering violation uuid=%d prevConsumed=%d", uuid, prevConsumed)
				return
			}

			// 3. No data loss + No phantom messages
			var lostUUIDs []int64
			phantom := false
			for lineUUID := prevConsumed + 1; lineUUID <= uuid; lineUUID++ {
				fileUUID, acked := readNextSafetyLine(reader)
				if fileUUID == -1 || fileUUID != lineUUID {
					phantom = true
					break
				}
				if fileUUID < uuid && acked {
					lostUUIDs = append(lostUUIDs, lineUUID)
					log.Printf("[safety] data loss: acked uuid=%d missing before uuid=%d", lineUUID, uuid)
				}
			}
			assert.Always(len(lostUUIDs) == 0, "No data loss", map[string]any{
				"receivedUUID": uuid, "lostUUIDs": lostUUIDs,
			})
			assert.Always(!phantom, "No phantom messages", map[string]any{
				"uuid": uuid,
			})
			if phantom {
				log.Printf("[safety] phantom message uuid=%d", uuid)
				return
			}

			// Advance state and commit
			lastConsumed = uuid
			persistConsumerState(lastConsumed)
			log.Printf("[safety] consumer state persisted uuid=%d", lastConsumed)
			commitCtx, commitCancel := context.WithTimeout(ctx, operationTimeout)
			client.CommitUncommittedOffsets(commitCtx)
			log.Printf("[safety] committed offsets uuid=%d", lastConsumed)
			commitCancel()
		})

		log.Printf("[safety] consumed %d records, lastConsumed=%d", fetches.NumRecords(), lastConsumed)
	}
}

func readNextSafetyLine(reader *bufio.Reader) (int64, bool) {
	var partial string
	deadline := time.Now().Add(operationTimeout)

	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		partial += line

		if err == nil {
			trimmed := strings.TrimRight(partial, "\n")
			parts := strings.SplitN(trimmed, ",", 2)
			if len(parts) == 2 {
				uuid, perr := strconv.ParseInt(parts[0], 10, 64)
				if perr == nil {
					return uuid, parts[1] == "1"
				}
			}
			log.Printf("[safety] malformed state line: %q", trimmed)
			partial = ""
			continue
		}

		if err == io.EOF {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		log.Printf("[safety] state file read error: %v", err)
		return -1, false
	}

	return -1, false
}

func persistConsumerState(lastConsumed int64) {
	tmp := safetyConsumerStateFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Printf("[safety] failed to write consumer state: %v", err)
		return
	}
	fmt.Fprintf(f, "%d\n", lastConsumed)
	f.Sync()
	f.Close()
	os.Rename(tmp, safetyConsumerStateFile)
}

func readConsumerState() int64 {
	data, err := os.ReadFile(safetyConsumerStateFile)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
