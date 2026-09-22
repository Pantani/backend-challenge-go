package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

func TestLoggerAddsContextAttributes(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(buf, "debug", "instance-1")
	ctx := observability.WithAttrs(context.Background(), slog.String("correlationId", "c-1"))
	ctx = observability.WithAttrs(ctx, slog.String("walletId", "w-1"))
	logger.With("static", 1).WithGroup("g").DebugContext(ctx, "hello", "k", "v")

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "hello", rec["msg"])
	assert.Equal(t, "wallet-service", rec["service"])
	assert.Equal(t, "instance-1", rec["instance"])
	group := rec["g"].(map[string]any)
	assert.Equal(t, "c-1", group["correlationId"])
	assert.Equal(t, "w-1", group["walletId"])
}

func TestLoggerLevels(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	observability.NewLogger(buf, "warn", "i").Info("hidden")
	assert.Empty(t, buf.String())
	observability.NewLogger(buf, "bogus", "i").Info("shown with the info fallback")
	assert.True(t, strings.Contains(buf.String(), "info fallback"))
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := observability.NewMetrics(reg)
	m.TransactionResult("http", wager.StatusProcessed, false)
	m.TransactionResult("sqs", wager.StatusProcessed, true)
	m.ConcurrencyConflict("submit")
	m.ProcessingDuration("http", time.Millisecond)
	m.ReconciliationDivergence()
	m.PendingResolution(wager.StatusRejected)
	m.SQSMessage("dlq")
	m.OutboxPublished()
	m.OutboxFailure()
	m.OutboxDeadLettered()
	m.OutboxLag(3 * time.Second)
	m.HTTPRequest("GET /x", 200, time.Millisecond)

	count, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)
	assert.Equal(t, 14, count)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP wager_duplicates_total Idempotent replays (duplicate deliveries) by source.
# TYPE wager_duplicates_total counter
wager_duplicates_total{source="sqs"} 1
`), "wager_duplicates_total"))
}
