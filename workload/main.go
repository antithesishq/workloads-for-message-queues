package main

import (
	"log"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

func main() {
	log.Println("workload: kafka cluster reported healthy via compose depends_on; signaling setup_complete")

	lifecycle.SetupComplete(map[string]any{
		"message": "3-node KRaft kafka cluster ready!",
	})
}
