package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/config"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	c, err := config.Load(config.MapLookup(nil))
	require.NoError(t, err)
	host, _ := os.Hostname()
	cases := []struct {
		name      string
		got, want any
	}{
		{"INSTANCE_ID", c.InstanceID, host},
		{"LOG_LEVEL", c.LogLevel, "info"},
		{"HTTP_ADDR", c.HTTPAddr, ":8080"},
		{"STARTUP_TIMEOUT", c.StartupTimeout, 25 * time.Second},
		{"SHUTDOWN_TIMEOUT", c.ShutdownTimeout, 40 * time.Second},
		{"READY_TIMEOUT", c.ReadyTimeout, 2 * time.Second},
		{"CONFLICT_RETRIES", c.ConflictRetries, 5},
		{"DATABASE_URL", c.DatabaseURL, "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"},
		{"DB_MAX_CONNS", c.DBMaxConns, 20},
		{"DB_LOCK_TIMEOUT", c.DBLockTimeout, 5 * time.Second},
		{"DB_STATEMENT_TIMEOUT", c.DBStatementTimeout, 10 * time.Second},
		{"OIDC_ISSUER", c.OIDCIssuer, "http://localhost:8180/realms/wallet"},
		{"OIDC_JWKS_URL", c.OIDCJWKSURL, "http://localhost:8180/realms/wallet/protocol/openid-connect/certs"},
		{"OIDC_AUDIENCE", c.OIDCAudience, "wallet-api"},
		{"AWS_REGION", c.AWSRegion, "us-east-1"},
		{"AWS_ENDPOINT_URL", c.AWSEndpoint, ""},
		{"SQS_INPUT_QUEUE", c.SQSInputQueue, "wager-transactions.fifo"},
		{"SQS_DLQ", c.SQSDLQ, "wager-transactions-dlq.fifo"},
		{"SQS_EVENTS_QUEUE", c.SQSEventsQueue, "wallet-events.fifo"},
		{"SQS_CONSUMER_NAME", c.SQSConsumerName, "wager-transactions-consumer"},
		{"SQS_CONSUMERS", c.SQSConsumers, 2},
		{"SQS_MAX_MESSAGES", c.SQSMaxMessages, 10},
		{"SQS_WAIT_TIME", c.SQSWaitTime, 10 * time.Second},
		{"SQS_VISIBILITY_TIMEOUT", c.SQSVisibilityTimeout, 5 * time.Minute},
		{"SQS_PROCESS_TIMEOUT", c.SQSProcessTimeout, 20 * time.Second},
		{"SQS_ACK_TIMEOUT", c.SQSAckTimeout, 5 * time.Second},
		{"SQS_RETRY_BASE", c.SQSRetryBase, 2 * time.Second},
		{"SQS_RETRY_MAX", c.SQSRetryMax, 60 * time.Second},
		{"SQS_MAX_RECEIVE_COUNT", c.SQSMaxReceiveCount, 5},
		{"SQS_SENDER_PROVIDERS", c.SQSSenderProviders, "000000000000=*"},
		{"PENDING_INTERVAL", c.PendingInterval, time.Second},
		{"PENDING_BASE_DELAY", c.PendingBaseDelay, time.Second},
		{"PENDING_MAX_DELAY", c.PendingMaxDelay, time.Minute},
		{"PENDING_MAX_ATTEMPTS", c.PendingMaxAttempts, 10},
		{"PENDING_BATCH", c.PendingBatch, 50},
		{"OUTBOX_INTERVAL", c.OutboxInterval, 500 * time.Millisecond},
		{"OUTBOX_BATCH", c.OutboxBatch, 50},
		{"OUTBOX_LEASE", c.OutboxLease, 30 * time.Second},
		{"OUTBOX_RETRY_BASE", c.OutboxRetryBase, time.Second},
		{"OUTBOX_RETRY_MAX", c.OutboxRetryMax, time.Minute},
		{"OUTBOX_PUBLISH_TIMEOUT", c.OutboxPublishTimeout, 10 * time.Second},
		{"OUTBOX_FINALIZE_TIMEOUT", c.OutboxFinalizeTimeout, 5 * time.Second},
		{"OUTBOX_MAX_ATTEMPTS", c.OutboxMaxAttempts, 20},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, tc.got, tc.name)
	}
}

func TestLoadRejectsUnrepresentableAdapterValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, key, value string
		load             func(config.Lookup) error
	}{
		{name: "pool count exceeds int32", key: "DB_MAX_CONNS", value: "2147483648", load: loadDatabase},
		{name: "lock timeout below one millisecond", key: "DB_LOCK_TIMEOUT", value: "999us", load: loadDatabase},
		{name: "lock timeout has fractional milliseconds", key: "DB_LOCK_TIMEOUT", value: "1500us", load: loadDatabase},
		{name: "statement timeout below one millisecond", key: "DB_STATEMENT_TIMEOUT", value: "999us", load: loadDatabase},
		{name: "statement timeout has fractional milliseconds", key: "DB_STATEMENT_TIMEOUT", value: "1500us", load: loadDatabase},
		{name: "wait time has fractional seconds", key: "SQS_WAIT_TIME", value: "1500ms", load: loadSQS},
		{name: "visibility has fractional seconds", key: "SQS_VISIBILITY_TIMEOUT", value: "300500ms", load: loadSQS},
		{name: "retry base has fractional seconds", key: "SQS_RETRY_BASE", value: "1500ms", load: loadSQS},
		{name: "retry max has fractional seconds", key: "SQS_RETRY_MAX", value: "60500ms", load: loadSQS},
		{name: "wait time exceeds API maximum", key: "SQS_WAIT_TIME", value: "21s", load: loadSQS},
		{name: "visibility exceeds API maximum", key: "SQS_VISIBILITY_TIMEOUT", value: "43201s", load: loadSQS},
		{name: "retry base exceeds API maximum", key: "SQS_RETRY_BASE", value: "43201s", load: loadSQS},
		{name: "retry max exceeds API maximum", key: "SQS_RETRY_MAX", value: "43201s", load: loadSQS},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.load(config.MapLookup(map[string]string{tt.key: tt.value}))
			require.ErrorContains(t, err, tt.key)
		})
	}
}

func TestLoadStartupTimeout(t *testing.T) {
	t.Parallel()
	c, err := config.Load(config.MapLookup(map[string]string{"STARTUP_TIMEOUT": "1ms"}))
	require.NoError(t, err, "short positive budgets remain valid cancellation paths")
	assert.Equal(t, time.Millisecond, c.StartupTimeout)

	for _, value := range []string{"0s", "-1s"} {
		_, err := config.Load(config.MapLookup(map[string]string{"STARTUP_TIMEOUT": value}))
		require.ErrorContains(t, err, "STARTUP_TIMEOUT")
	}
}

func TestLoadRejectsOverflowingDurationRelationships(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		values map[string]string
	}{
		{
			name: "SQS drain addition overflows",
			values: map[string]string{
				"SQS_MAX_MESSAGES": "1", "SQS_PROCESS_TIMEOUT": "2562047h47m16.854775807s",
				"SQS_ACK_TIMEOUT": "1ns", "SHUTDOWN_TIMEOUT": "2562047h47m16.854775807s",
			},
		},
		{
			name: "outbox drain addition overflows",
			values: map[string]string{
				"OUTBOX_PUBLISH_TIMEOUT": "2562047h47m16.854775806s", "OUTBOX_FINALIZE_TIMEOUT": "2ns",
				"OUTBOX_LEASE": "2562047h47m16.854775807s", "SHUTDOWN_TIMEOUT": "2562047h47m16.854775807s",
			},
		},
		{
			name: "statement timeout exceeds drain near maximum duration",
			values: map[string]string{
				"DB_STATEMENT_TIMEOUT": "2562047h47m16.854s", "SHUTDOWN_TIMEOUT": "2562047h47m16.854775807s",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(config.MapLookup(tt.values))
			require.ErrorContains(t, err, "SHUTDOWN_TIMEOUT")
		})
	}
}

func TestShutdownDrainCoversConcurrentOperations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		values map[string]string
	}{
		{name: "HTTP write", values: map[string]string{"SHUTDOWN_TIMEOUT": "35s"}},
		{name: "SQS process and acknowledgement", values: map[string]string{
			"SHUTDOWN_TIMEOUT": "40s", "SQS_PROCESS_TIMEOUT": "30s", "SQS_ACK_TIMEOUT": "5s",
			"SQS_MAX_MESSAGES": "1", "SQS_VISIBILITY_TIMEOUT": "36s",
		}},
		{name: "outbox publish and finalize", values: map[string]string{
			"SHUTDOWN_TIMEOUT": "40s", "OUTBOX_PUBLISH_TIMEOUT": "30s", "OUTBOX_FINALIZE_TIMEOUT": "5s",
			"OUTBOX_LEASE": "36s",
		}},
		{name: "database statement", values: map[string]string{
			"SHUTDOWN_TIMEOUT": "40s", "DB_STATEMENT_TIMEOUT": "35s",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(config.MapLookup(tt.values))
			require.ErrorContains(t, err, "SHUTDOWN_TIMEOUT")
		})
	}
}

func loadDatabase(lookup config.Lookup) error {
	_, err := config.LoadDatabase(lookup)
	return err
}

func loadSQS(lookup config.Lookup) error {
	_, err := config.LoadSQS(lookup)
	return err
}

func TestRejectsVisibilityShorterThanWholeBatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		visibility string
	}{
		{name: "shorter", visibility: "30s"},
		{name: "equal", visibility: "250s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(config.MapLookup(map[string]string{
				"SQS_MAX_MESSAGES":       "10",
				"SQS_PROCESS_TIMEOUT":    "20s",
				"SQS_ACK_TIMEOUT":        "5s",
				"SQS_VISIBILITY_TIMEOUT": tt.visibility,
			}))
			require.ErrorContains(t, err, "whole receive batch")
		})
	}
}

func TestRejectsSQSDurationsThatLosePrecisionAtTheAdapter(t *testing.T) {
	t.Parallel()
	tests := map[string]map[string]string{
		"visibility above exact batch budget": {
			"SQS_MAX_MESSAGES":       "2",
			"SQS_PROCESS_TIMEOUT":    "2.9s",
			"SQS_ACK_TIMEOUT":        "50ms",
			"SQS_VISIBILITY_TIMEOUT": "5.95s",
		},
		"subsecond wait": {
			"SQS_WAIT_TIME": "500ms",
		},
		"fractional retry base": {
			"SQS_RETRY_BASE": "1.5s",
		},
		"fractional retry max": {
			"SQS_RETRY_MAX": "60.5s",
		},
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadSQS(config.MapLookup(values))
			require.ErrorContains(t, err, "whole seconds")
		})
	}
}

func TestRejectsWholeBatchBudgetOverflow(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, process, ack string
	}{
		{name: "addition", process: "2562047h47m16.854775807s", ack: "1ns"},
		{name: "multiplication", process: "1000000h", ack: "1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(config.MapLookup(map[string]string{
				"SQS_MAX_MESSAGES":       "10",
				"SQS_PROCESS_TIMEOUT":    tt.process,
				"SQS_ACK_TIMEOUT":        tt.ack,
				"SQS_VISIBILITY_TIMEOUT": "12h",
			}))
			require.ErrorContains(t, err, "whole receive batch")
		})
	}
}

func TestOverrides(t *testing.T) {
	t.Parallel()
	c, err := config.Load(config.MapLookup(map[string]string{"HTTP_ADDR": ":9090", "SQS_CONSUMERS": "4", "OUTBOX_LEASE": "5s",
		"INSTANCE_ID": "i-1", "LOG_LEVEL": "", "CONFLICT_RETRIES": "0", "OUTBOX_PUBLISH_TIMEOUT": "2s",
		"OUTBOX_FINALIZE_TIMEOUT": "2s"}))
	require.NoError(t, err)
	assert.Equal(t, ":9090", c.HTTPAddr)
	assert.Equal(t, 4, c.SQSConsumers)
	assert.Equal(t, 5*time.Second, c.OutboxLease)
	assert.Equal(t, "i-1", c.InstanceID)
	assert.Equal(t, "info", c.LogLevel, "empty values fall back to defaults")
	assert.Zero(t, c.ConflictRetries, "zero retries is allowed")
	assert.Equal(t, 2*time.Second, c.OutboxPublishTimeout)
	assert.Equal(t, 2*time.Second, c.OutboxFinalizeTimeout)
}

func TestOutboxBudgetsFitOneLeaseWithoutBatchMultiplier(t *testing.T) {
	t.Parallel()
	_, err := config.Load(config.MapLookup(map[string]string{
		"OUTBOX_BATCH": "1000", "OUTBOX_PUBLISH_TIMEOUT": "10s", "OUTBOX_FINALIZE_TIMEOUT": "5s", "OUTBOX_LEASE": "16s",
	}))
	require.NoError(t, err)
}

func TestRejectsOutboxBudgetThatDoesNotFitLease(t *testing.T) {
	t.Parallel()
	tests := map[string]map[string]string{
		"equal to lease": {
			"OUTBOX_PUBLISH_TIMEOUT": "10s", "OUTBOX_FINALIZE_TIMEOUT": "20s", "OUTBOX_LEASE": "30s",
		},
		"addition overflow": {
			"OUTBOX_PUBLISH_TIMEOUT": "2562047h47m16.854775805s", "OUTBOX_FINALIZE_TIMEOUT": "3ns",
			"OUTBOX_LEASE": "2562047h47m16.854775807s", "SHUTDOWN_TIMEOUT": "2562047h47m16.854775807s",
		},
	}
	for name, values := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load(config.MapLookup(values))
			require.ErrorContains(t, err, "OUTBOX_PUBLISH_TIMEOUT plus OUTBOX_FINALIZE_TIMEOUT")
		})
	}
}

func TestParseErrorsAreAggregated(t *testing.T) {
	t.Parallel()
	_, err := config.Load(config.MapLookup(map[string]string{"DB_MAX_CONNS": "many", "OUTBOX_LEASE": "forever"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DB_MAX_CONNS")
	assert.Contains(t, err.Error(), "OUTBOX_LEASE")
}

func TestValidation(t *testing.T) {
	t.Parallel()
	invalid := map[string]map[string]string{
		"negative pool":                  {"DB_MAX_CONNS": "-1"},
		"no consumers":                   {"SQS_CONSUMERS": "0"},
		"batch too big":                  {"SQS_MAX_MESSAGES": "11"},
		"wait too long":                  {"SQS_WAIT_TIME": "21s"},
		"process >= visibility":          {"SQS_PROCESS_TIMEOUT": "30s"},
		"pending base > max":             {"PENDING_BASE_DELAY": "2m"},
		"zero pending interval":          {"PENDING_INTERVAL": "0s"},
		"negative outbox interval":       {"OUTBOX_INTERVAL": "-1s"},
		"zero lease":                     {"OUTBOX_LEASE": "0s"},
		"negative wait":                  {"SQS_WAIT_TIME": "-5s"},
		"zero sqs retry max":             {"SQS_RETRY_MAX": "0s"},
		"negative outbox retry max":      {"OUTBOX_RETRY_MAX": "-1s"},
		"zero lock timeout":              {"DB_LOCK_TIMEOUT": "0s"},
		"negative statement":             {"DB_STATEMENT_TIMEOUT": "-1s"},
		"zero outbox attempts":           {"OUTBOX_MAX_ATTEMPTS": "0"},
		"zero receive count":             {"SQS_MAX_RECEIVE_COUNT": "0"},
		"negative conflict retries":      {"CONFLICT_RETRIES": "-1"},
		"bad log level":                  {"LOG_LEVEL": "loud"},
		"sqs retry base > max":           {"SQS_RETRY_BASE": "2m"},
		"outbox retry base > max":        {"OUTBOX_RETRY_BASE": "2m"},
		"visibility > 12h":               {"SQS_VISIBILITY_TIMEOUT": "13h"},
		"sqs retry max > 12h":            {"SQS_RETRY_MAX": "13h"},
		"shutdown <= process":            {"SHUTDOWN_TIMEOUT": "20s"},
		"shutdown <= process + ack":      {"SHUTDOWN_TIMEOUT": "25s"},
		"shutdown <= publish":            {"SHUTDOWN_TIMEOUT": "29s", "OUTBOX_PUBLISH_TIMEOUT": "29s", "OUTBOX_LEASE": "40s"},
		"zero publish timeout":           {"OUTBOX_PUBLISH_TIMEOUT": "0s"},
		"zero finalize timeout":          {"OUTBOX_FINALIZE_TIMEOUT": "0s"},
		"publish plus finalize >= lease": {"OUTBOX_PUBLISH_TIMEOUT": "25s"},
		"process + ack >= visible":       {"SQS_ACK_TIMEOUT": "10s"},
		"zero ack timeout":               {"SQS_ACK_TIMEOUT": "0s"},
	}
	for name, values := range invalid {
		_, err := config.Load(config.MapLookup(values))
		assert.Error(t, err, name)
	}
}

func TestValidationMessages(t *testing.T) {
	t.Parallel()
	_, err := config.Load(config.MapLookup(map[string]string{"LOG_LEVEL": "loud", "SHUTDOWN_TIMEOUT": "1s"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LOG_LEVEL")
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLogLevelIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	c, err := config.Load(config.MapLookup(map[string]string{"LOG_LEVEL": "WARN"}))
	require.NoError(t, err)
	assert.Equal(t, "WARN", c.LogLevel)
}

func TestZeroWaitTimeIsValid(t *testing.T) {
	t.Parallel()
	c, err := config.Load(config.MapLookup(map[string]string{"SQS_WAIT_TIME": "0s"}))
	require.NoError(t, err)
	assert.Zero(t, c.SQSWaitTime, "short polling is allowed")
}

func TestLoadDatabase(t *testing.T) {
	t.Parallel()
	d, err := config.LoadDatabase(config.MapLookup(map[string]string{"DATABASE_URL": "postgres://x", "SQS_CONSUMERS": "0", "LOG_LEVEL": "loud"}))
	require.NoError(t, err, "only database variables are read")
	assert.Equal(t, "postgres://x", d.DatabaseURL)
	assert.Equal(t, 20, d.DBMaxConns)

	_, err = config.LoadDatabase(config.MapLookup(map[string]string{"DB_MAX_CONNS": "0"}))
	require.Error(t, err)
	_, err = config.LoadDatabase(config.MapLookup(map[string]string{"DB_LOCK_TIMEOUT": "soon"}))
	require.Error(t, err)
}

func TestLoadSQS(t *testing.T) {
	t.Parallel()
	s, err := config.LoadSQS(config.MapLookup(map[string]string{"SQS_DLQ": "d.fifo", "DB_MAX_CONNS": "0", "OUTBOX_LEASE": "0s"}))
	require.NoError(t, err, "only AWS variables are read")
	assert.Equal(t, "d.fifo", s.SQSDLQ)
	assert.Equal(t, 5, s.SQSMaxReceiveCount)

	_, err = config.LoadSQS(config.MapLookup(map[string]string{"SQS_MAX_MESSAGES": "0"}))
	require.Error(t, err)
	_, err = config.LoadSQS(config.MapLookup(map[string]string{"SQS_CONSUMERS": "two"}))
	require.Error(t, err)
}

func TestFromEnv(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":7070")
	c, err := config.FromEnv()
	require.NoError(t, err)
	assert.Equal(t, ":7070", c.HTTPAddr)
}
