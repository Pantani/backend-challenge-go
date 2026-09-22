package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/config"
)

func env(values map[string]string) config.Lookup {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	c, err := config.Load(env(nil))
	require.NoError(t, err)
	assert.Equal(t, ":8080", c.HTTPAddr)
	assert.Equal(t, "wager-transactions.fifo", c.SQSInputQueue)
	assert.Equal(t, "wager-transactions-dlq.fifo", c.SQSDLQ)
	assert.Equal(t, 10, c.PendingMaxAttempts)
	assert.Equal(t, 30*time.Second, c.SQSVisibility)
	assert.NotEmpty(t, c.InstanceID)
	assert.Equal(t, 20, c.OutboxMaxTries)
	assert.Equal(t, "000000000000=*", c.SQSSenderProviders, "LocalStack reports every sender as the account id")
}

func TestOverrides(t *testing.T) {
	t.Parallel()
	c, err := config.Load(env(map[string]string{"HTTP_ADDR": ":9090", "SQS_CONSUMERS": "4", "OUTBOX_LEASE": "5s", "INSTANCE_ID": "i-1", "LOG_LEVEL": ""}))
	require.NoError(t, err)
	assert.Equal(t, ":9090", c.HTTPAddr)
	assert.Equal(t, 4, c.SQSConsumers)
	assert.Equal(t, 5*time.Second, c.OutboxLease)
	assert.Equal(t, "i-1", c.InstanceID)
	assert.Equal(t, "info", c.LogLevel, "empty values fall back to defaults")
}

func TestParseErrorsAreAggregated(t *testing.T) {
	t.Parallel()
	_, err := config.Load(env(map[string]string{"DB_MAX_CONNS": "many", "OUTBOX_LEASE": "forever"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DB_MAX_CONNS")
	assert.Contains(t, err.Error(), "OUTBOX_LEASE")
}

func TestValidation(t *testing.T) {
	t.Parallel()
	invalid := []map[string]string{
		{"DB_MAX_CONNS": "-1"},
		{"SQS_CONSUMERS": "0"},
		{"SQS_MAX_MESSAGES": "11"},
		{"SQS_WAIT_TIME": "21s"},
		{"SQS_PROCESS_TIMEOUT": "30s"},
		{"PENDING_BASE_DELAY": "2m"},
		{"PENDING_INTERVAL": "0s"},
		{"OUTBOX_INTERVAL": "-1s"},
		{"OUTBOX_LEASE": "0s"},
		{"SQS_WAIT_TIME": "-5s"},
		{"SQS_RETRY_MAX": "0s"},
		{"OUTBOX_RETRY_MAX": "-1s"},
		{"DB_LOCK_TIMEOUT": "0s"},
		{"DB_STATEMENT_TIMEOUT": "-1s"},
		{"OUTBOX_MAX_ATTEMPTS": "0"},
		{"SQS_MAX_RECEIVE_COUNT": "0"},
	}
	for _, values := range invalid {
		_, err := config.Load(env(values))
		assert.Error(t, err, values)
	}
}

func TestZeroWaitTimeIsValid(t *testing.T) {
	t.Parallel()
	c, err := config.Load(env(map[string]string{"SQS_WAIT_TIME": "0s"}))
	require.NoError(t, err)
	assert.Zero(t, c.SQSWaitTime, "short polling is allowed")
}

func TestFromEnv(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":7070")
	c, err := config.FromEnv()
	require.NoError(t, err)
	assert.Equal(t, ":7070", c.HTTPAddr)
}
