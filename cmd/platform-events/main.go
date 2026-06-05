// Command platform-events is a reference CLI for the platform-events library.
// It validates environment configuration, initialises metrics, and prints the
// loaded config. Consuming services should follow the same startup sequence
// before wiring publisher, consumer, and outbox runner.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

// version is set by the build system via -ldflags.
var version = "dev"

func main() {
	strict := flag.Bool("strict", false, "exit non-zero when required env vars are missing")
	flag.Parse()

	appName := os.Getenv("APP_NAME")
	if appName == "" {
		appName = "platform-events"
	}
	buildVersion := os.Getenv("BUILD_VERSION")
	if buildVersion == "" {
		buildVersion = version
	}
	events.Init(appName, buildVersion)

	fmt.Fprintf(os.Stdout, "platform-events %s\n", buildVersion)

	snsCfg := config.LoadSNS()
	sqsCfg := config.LoadSQS()
	outboxCfg := config.LoadOutbox()
	otelCfg := config.LoadOTel()

	config.LogWarnings(sqsCfg.Warnings)
	config.LogWarnings(outboxCfg.Warnings)

	var problems []string
	if err := snsCfg.Validate(); err != nil {
		problems = append(problems, err.Error())
	}
	if err := sqsCfg.Validate(); err != nil {
		problems = append(problems, err.Error())
	}
	if err := outboxCfg.Validate(); err != nil {
		problems = append(problems, err.Error())
	}

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
	fmt.Fprintf(os.Stdout, "  MaxReceiveCount:   %d\n", sqsCfg.MaxReceiveCount)
	if sqsCfg.MaxReceiveCount == 0 {
		fmt.Fprintln(os.Stdout, "  (apply events.WithMaxReceiveCount when wiring WithDeadLetterHandler)")
	}

	fmt.Fprintln(os.Stdout, "\n=== Outbox Config ===")
	fmt.Fprintf(os.Stdout, "  %s\n", outboxCfg.String())
	if outboxCfg.ClaimLeaseDuration == 0 {
		fmt.Fprintln(os.Stdout, "  ClaimLeaseDuration: 10m (store default when OUTBOX_CLAIM_LEASE_DURATION unset)")
	}

	fmt.Fprintln(os.Stdout, "\n=== OTel Config ===")
	fmt.Fprintf(os.Stdout, "  ServiceName:      %s\n", otelCfg.ServiceName)
	fmt.Fprintf(os.Stdout, "  ExporterEndpoint: %s\n", otelCfg.ExporterEndpoint)
	fmt.Fprintf(os.Stdout, "  Insecure:         %v\n", otelCfg.Insecure)

	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "\n=== Configuration problems ===")
		for _, p := range problems {
			fmt.Fprintf(os.Stderr, "  - %s\n", p)
		}
		if *strict {
			os.Exit(1)
		}
	}

	fmt.Fprintln(os.Stdout, "\n=== Production wiring reminders ===")
	reminders := []string{
		"outbox.ApplySchema(ctx, migrateRunner) on startup",
		"events.Init(serviceName, buildVersion) once at startup (done by this CLI)",
		"gincommon.InitTracingFromEnv() before first Publish/Start",
		"SNS→SQS subscription MUST use RawMessageDelivery=true",
		"outbox.Runner.Ready() before marking the pod ready",
		"Transactional events: outbox.Enqueue inside pgcommon.RunInTx only",
		"Consumer handlers: idempotency on Envelope.ID",
	}
	for _, r := range reminders {
		fmt.Fprintf(os.Stdout, "  - %s\n", r)
	}
}
