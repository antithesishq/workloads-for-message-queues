package main

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

func main() {
	ctx := context.Background()

	log.Println("workload: kafka cluster reported healthy via compose depends_on; signaling setup_complete")
	lifecycle.SetupComplete("3-node KRaft kafka cluster ready!")

	go createTopic(ctx)
	go produce(ctx)
	go consume(ctx)

	time.Sleep(math.MaxInt64)
}
