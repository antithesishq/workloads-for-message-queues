package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

func main() {
	ctx := context.Background()
	lifecycle.SetupComplete("kafka cluster ready")

	go runSafety(ctx)
	log.Println("started workload: safety")

	go runLiveness(ctx)
	log.Println("started workload: liveness")

	go runAtLeastOnce(ctx)
	log.Println("started workload: at-least-once")

	go runAtMostOnce(ctx)
	log.Println("started workload: at-most-once")

	select {}
}
