// Package cli implements the wallet binary commands:
//
//	wallet serve                 run the API, consumers and workers (default)
//	wallet migrate up            apply every pending migration
//	wallet migrate down [n]      revert n migrations (default 1)
//	wallet migrate version       print the current schema version
//	wallet provision-queues      create the SQS queues and the DLQ redrive
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Pantani/backend-challenge-go/internal/adapter/postgres"
	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/config"
)

// ErrUsage reports an unknown command or bad arguments.
var ErrUsage = errors.New("usage: wallet [serve | migrate up | migrate down [n] | migrate version | provision-queues]")

// Run executes the command in args and returns the process exit code.
func Run(ctx context.Context, args []string, lookup config.Lookup, stdout io.Writer) int {
	if err := run(ctx, args, lookup, stdout); err != nil {
		_, _ = fmt.Fprintln(stdout, "error:", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, lookup config.Lookup, stdout io.Writer) error {
	cfg, err := config.Load(lookup)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	cmd, rest := "serve", []string(nil)
	if len(args) > 0 {
		cmd, rest = args[0], args[1:]
	}
	switch cmd {
	case "serve":
		return serve(ctx, cfg)
	case "migrate":
		return migrateCmd(cfg, rest, stdout)
	case "provision-queues":
		return provision(ctx, cfg, stdout)
	}
	return ErrUsage
}

// serve runs until SIGINT/SIGTERM (or ctx cancellation), then stops the
// application within the configured shutdown timeout.
func serve(ctx context.Context, cfg config.Config) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	application := bootstrap.New(cfg)
	if err := application.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	return application.Stop(stopCtx)
}

func migrateCmd(cfg config.Config, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return ErrUsage
	}
	m, err := postgres.NewMigrator(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	switch args[0] {
	case "up":
		return m.Up()
	case "down":
		return migrateDown(m, args[1:])
	case "version":
		return printVersion(m, stdout)
	}
	return ErrUsage
}

func migrateDown(m *postgres.Migrator, args []string) error {
	steps := 1
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 {
			return ErrUsage
		}
		steps = n
	}
	return m.Down(steps)
}

type versioner interface {
	Version() (uint, bool, error)
}

func printVersion(m versioner, stdout io.Writer) error {
	v, dirty, err := m.Version()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "version=%d dirty=%t\n", v, dirty)
	return err
}

func provision(ctx context.Context, cfg config.Config, stdout io.Writer) error {
	client, err := sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint})
	if err != nil {
		return err
	}
	q, err := sqsadapter.Provision(ctx, client, sqsadapter.ProvisionConfig{
		Names:           sqsadapter.QueueNames{Input: cfg.SQSInputQueue, DLQ: cfg.SQSDLQ, Events: cfg.SQSEventsQueue},
		MaxReceiveCount: cfg.SQSMaxReceive, VisibilityTimeout: int(cfg.SQSVisibility.Seconds()),
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "input=%s\ndlq=%s\nevents=%s\n", q.Input, q.DLQ, q.Events)
	return err
}
