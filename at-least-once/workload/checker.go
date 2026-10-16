package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

type checker struct {
	mu       sync.Mutex
	attempts map[string]int // how many times we tried to send each uuid
	acks     map[string]int // how many times each uuid was acknowledged
	consumed map[string]int // how many times each uuid was consumed
	// Highest counter seen in the acked/consumed streams. Updated under mu.
	lastConsumed int64 // highest counter the consumer has seen so far
	highestAcked int64 // highest counter the producer has gotten an ack for
	totalAcks    int64
	totalReads   int64 // includes phantom records
}

func newChecker() *checker {
	return &checker{
		attempts: map[string]int{},
		acks:     map[string]int{},
		consumed: map[string]int{},
	}
}

func (c *checker) attempted(uuid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts[uuid]++
}

func (c *checker) acked(uuid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks[uuid]++
	if counter, ok := counterFromUUID(uuid); ok && counter > c.highestAcked {
		c.highestAcked = counter
	}
	c.totalAcks++
}

func (c *checker) checkConsumed(uuid string, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totalReads++
	details := map[string]any{"uuid": uuid, "offset": offset}

	// Phantom
	counter, parsed := counterFromUUID(uuid)

	if !parsed {
		log.Printf("couldn't parse the uuid: %v. skipping it.", uuid)
		return
	}

	produced := c.attempts[uuid] > 0
	assert.Always(produced,
		"No phantom messages: every consumed message was sent by the producer",
		details)
	if !produced {
		return
	}

	// Ordering (equal is OK because a retried message can produce the same counter twice at different offsets)
	details["previous_counter"] = c.lastConsumed
	assert.Always(counter >= c.lastConsumed,
		"Message ordering: messages are consumed in the order they were produced",
		details)

	// Data loss
	var skipped []int64
	for k := c.lastConsumed + 1; k < counter; k++ {
		kuuid := fmt.Sprintf("%s-%d", producerID, k)
		if c.acks[kuuid] > 0 {
			skipped = append(skipped, k)
		}
	}
	details["skipped_acked_counters"] = skipped
	assert.Always(len(skipped) == 0,
		"No data loss: the consumer never skips past an acknowledged message",
		details)

	// Update state after ordering/data-loss checks so this record isn't counted against itself.
	c.consumed[uuid]++
	if counter > c.lastConsumed {
		c.lastConsumed = counter
	}

	// Delivery guarantee: a message is consumed at most as many times as it was sent
	details["consumed"] = c.consumed[uuid]
	details["attempts"] = c.attempts[uuid]
	assert.AlwaysLessThanOrEqualTo(c.consumed[uuid], c.attempts[uuid],
		"Delivery guarantee (at least once): a message is consumed no more times than it was sent",
		details)

	// Duplicate: at least once allows the consumer to see a retried message twice.
	if c.consumed[uuid] > 1 {
		if c.acks[uuid] > 1 {
			assert.Reachable("Delivery guarantee (at least once): a retried message was consumed more than once",
			details)
		} else {
			details["acked"] = c.acks
			assert.Unreachable("Phantom duplicate message", details)
		}
		
	}
}

func (c *checker) runFaultFreeWindows(interval, duration time.Duration) {
	for {
		time.Sleep(interval)

		c.mu.Lock()
		acksBefore := c.totalAcks
		readsBefore := c.totalReads
		c.mu.Unlock()

		start := time.Now()
		if cmd := os.Getenv("ANTITHESIS_STOP_FAULTS"); cmd != "" {
			out, err := exec.Command(cmd, strconv.Itoa(int(duration.Seconds()))).CombinedOutput()
			if err != nil {
				log.Fatalf("ANTITHESIS_STOP_FAULTS failed: %v: %s", err, out)
			}
		} else {
			log.Fatalf("ANTITHESIS_STOP_FAULTS not set")
		}
		assert.Reachable("A fault-free window started",
			map[string]any{"duration_seconds": int(duration.Seconds())})

		// Wait for the three progress indicators to advance
		var acksNow, readsNow int64
		for {
			c.mu.Lock()
			acksNow = c.totalAcks
			readsNow = c.totalReads
			c.mu.Unlock()
			if acksNow > acksBefore && readsNow > readsBefore {
				break
			}
			if time.Since(start) > duration-time.Second {
				break
			}
			time.Sleep(time.Second)
		}

		waited := time.Since(start)
		details := map[string]any{
			"waited_seconds":          int(waited.Seconds()),
			"acks_before":             acksBefore,
			"acks_after":              acksNow,
			"reads_before":            readsBefore,
			"reads_after":             readsNow,
		}
		assert.Always(acksNow > acksBefore,
			"Producer progress: the producer gets an ack during every fault-free window",
			details)
		assert.Always(readsNow > readsBefore,
			"Consumer progress: the consumer reads a message during every fault-free window",
			details)

	}
}

func counterFromUUID(uuid string) (int64, bool) {
	rest, ok := strings.CutPrefix(uuid, producerID+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}