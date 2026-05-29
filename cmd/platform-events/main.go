// Command platform-events is a reference CLI for the platform-events library.
// It prints the current configuration as loaded from environment variables
// and verifies that required environment variables are set.
package main

import (
	"fmt"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/internal/config"
)

// version is set by the build system via -ldflags.
var version = "dev"

func main() {
	fmt.Fprintf(os.Stdout, "platform-events %s\n", version)

	snsCfg := config.LoadSNS()
	sqsCfg := config.LoadSQS()
	outboxCfg := config.LoadOutbox()
	otelCfg := config.LoadOTel()

	fmt.Fprintln(os.Stdout, "\n=== SNS Config ===")
	fmt.Fprintf(os.Stdout, "  TopicARN:    %s\n", snsCfg.TopicARN)
	fmt.Fprintf(os.Stdout, "  Region:      %s\n", snsCfg.Region)
	fmt.Fprintf(os.Stdout, "  EndpointURL: %s\n", snsCfg.EndpointURL)

	fmt.Fprintln(os.Stdout, "\n=== SQS Config ===")
	fmt.Fprintf(os.Stdout, "  QueueURL:          %s\n", sqsCfg.QueueURL)
	fmt.Fprintf(os.Stdout, "  Region:            %s\n", sqsCfg.Region)
	fmt.Fprintf(os.Stdout, "  MaxMessages:       %d\n", sqsCfg.MaxMessages)
	fmt.Fprintf(os.Stdout, "  WaitSeconds:       %d\n", sqsCfg.WaitSeconds)
	fmt.Fprintf(os.Stdout, "  VisibilityTimeout: %s\n", sqsCfg.VisibilityTimeout)
	fmt.Fprintf(os.Stdout, "  Concurrency:       %d\n", sqsCfg.Concurrency)

	fmt.Fprintln(os.Stdout, "\n=== Outbox Config ===")
	fmt.Fprintf(os.Stdout, "  PollInterval: %s\n", outboxCfg.PollInterval)
	fmt.Fprintf(os.Stdout, "  BatchSize:    %d\n", outboxCfg.BatchSize)
	fmt.Fprintf(os.Stdout, "  MaxAttempts:  %d\n", outboxCfg.MaxAttempts)

	fmt.Fprintln(os.Stdout, "\n=== OTel Config ===")
	fmt.Fprintf(os.Stdout, "  ServiceName:      %s\n", otelCfg.ServiceName)
	fmt.Fprintf(os.Stdout, "  ExporterEndpoint: %s\n", otelCfg.ExporterEndpoint)
	fmt.Fprintf(os.Stdout, "  Insecure:         %v\n", otelCfg.Insecure)
}
