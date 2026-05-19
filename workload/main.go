package main

import (
	"log"
	"math"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

func main() {
	log.Println("workload: kafka cluster reported healthy via compose depends_on; signaling setup_complete")

	lifecycle.SetupComplete("3-node KRaft kafka cluster ready!")
	time.Sleep(math.MaxInt64)
}
