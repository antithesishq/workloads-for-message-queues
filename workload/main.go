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

	createTopic(ctx)
	produce(ctx)
	consume(ctx)

	log.Println("workload: test complete, keeping container alive")
	time.Sleep(math.MaxInt64)
}
