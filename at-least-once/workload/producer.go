package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producer struct {
	client   *kgo.Client
	stateLog *os.File
	checker  *checker
	nextSeq     int64
}

func newProducerClient(brokers []string) *kgo.Client {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// Without a delivery timeout franz-go retries forever but we need visible failures.
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		log.Fatalf("new producer client: %v", err)
	}
	return client
}

func (p *producer) run() {
	for {
		p.send(p.nextSeq)
		p.nextSeq++
		time.Sleep(time.Duration(random.GetRandom()%50) * time.Millisecond)
	}
}

func (p *producer) send(counter int64) {
	uuid := fmt.Sprintf("%s-%d", producerID, counter)

	// Record body: 100-200 chars from the alphanumeric alphabet below. Each
	// character is picked via random.RandomChoice so Antithesis can steer it.
	alphabet := []any{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M",
		"N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
		"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
	}
	bodyLen := 100 + int(random.GetRandom()%1000)
	body := make([]byte, bodyLen)
	for i := 0; i < bodyLen; i++ {
		body[i] = random.RandomChoice(alphabet).(string)[0]
	}
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])

	for attempt := 1; attempt <= 1+maxRetries; attempt++ {
		if attempt > 1 {
			assert.Reachable("Producer retried a failed send", map[string]any{"uuid": uuid})
		}

		// Record the attempt before sending so a crash mid-send still shows the
		// attempt in the state file; the checker uses it to recognize phantoms.
		writeLine(p.stateLog, "%s, attempt", uuid)
		p.checker.attempted(uuid)

		rec := &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(uuid),
			Value:     body,
			Headers:   []kgo.RecordHeader{{Key: "checksum", Value: []byte(checksum)}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		res := p.client.ProduceSync(ctx, rec)
		cancel()
		if err := res.FirstErr(); err != nil {
			log.Printf("send %s attempt %d failed: %v", uuid, attempt, err)
			assert.Reachable("Producer send attempt failed", map[string]any{"uuid": uuid})
			time.Sleep(500 * time.Millisecond)
			continue
		}
		writeLine(p.stateLog, "%s, ack", uuid)
		p.checker.acked(uuid)
		return
	}
	assert.Reachable("Producer gave up on a message after the maximum number of retries",
		map[string]any{"uuid": uuid})
}
