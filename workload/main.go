package main

import (
	"context"
	"log"
	mathrand "math/rand/v2"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

type workload struct {
	name string
	run  func(context.Context)
}

var workloads = []workload{
	{"safety", runSafety},
	{"at-most-once", runAtMostOnce},
	{"consumer-progress", runConsumerProgress},
	{"at-least-once", runAtLeastOnce},
}

func main() {
	ctx := context.Background()
	lifecycle.SetupComplete("kafka cluster ready")

	for {
		w := workloads[mathrand.IntN(len(workloads))]
		log.Printf("selected workload: %s\n", w.name)
		w.run(ctx)
		log.Printf("workload %s completed\n", w.name)
	}
}
