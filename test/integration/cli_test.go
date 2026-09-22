//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/cli"
)

func lookup(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}
}

func runCLI(ctx context.Context, vars map[string]string, args ...string) (int, string) {
	var out bytes.Buffer
	code := cli.Run(ctx, args, lookup(env.Vars(vars)), &out)
	return code, out.String()
}

func TestCLIMigrations(t *testing.T) {
	t.Parallel()
	_, err := pool.Exec(context.Background(), `CREATE DATABASE cli_check`)
	require.NoError(t, err)
	db := map[string]string{"DATABASE_URL": strings.Replace(env.DatabaseURL, "/wallet?", "/cli_check?", 1)}
	ctx := context.Background()

	code, out := runCLI(ctx, db, "migrate", "up")
	require.Zero(t, code, out)
	code, out = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, "version=2 dirty=false\n", out)
	code, _ = runCLI(ctx, db, "migrate", "down", "1")
	require.Zero(t, code)
	code, out = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, "version=1 dirty=false\n", out)
	code, _ = runCLI(ctx, db, "migrate", "down", "1")
	require.Zero(t, code)
	code, out = runCLI(ctx, db, "migrate", "version")
	require.Zero(t, code)
	assert.Equal(t, "version=0 dirty=false\n", out)
	code, _ = runCLI(ctx, db, "migrate", "down")
	assert.Zero(t, code, "nothing left to revert is not an error")

	for _, args := range [][]string{{"migrate"}, {"migrate", "sideways"}, {"migrate", "down", "zero"}, {"unknown"}} {
		code, out := runCLI(ctx, db, args...)
		assert.Equal(t, 1, code, args)
		assert.Contains(t, out, "usage", args)
	}
	code, out = runCLI(ctx, map[string]string{"DATABASE_URL": "postgres://%%%"}, "migrate", "up")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "error")
	code, out = runCLI(ctx, map[string]string{"SQS_CONSUMERS": "0"}, "migrate", "up")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "invalid configuration")
}

func TestCLIProvisionQueues(t *testing.T) {
	t.Parallel()
	vars := map[string]string{"SQS_INPUT_QUEUE": "cli-in.fifo", "SQS_DLQ": "cli-dlq.fifo", "SQS_EVENTS_QUEUE": "cli-events.fifo"}
	code, out := runCLI(context.Background(), vars, "provision-queues")
	require.Zero(t, code, out)
	assert.Contains(t, out, "cli-dlq.fifo")

	code, _ = runCLI(context.Background(), map[string]string{"SQS_INPUT_QUEUE": "not-fifo"}, "provision-queues")
	assert.Equal(t, 1, code, "a FIFO queue name must end in .fifo")
}

func TestCLIServeUntilCancelled(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		code, _ := runCLI(ctx, queueVars(names))
		done <- code
	}()
	time.Sleep(2 * time.Second)
	cancel()
	select {
	case code := <-done:
		assert.Zero(t, code, "graceful shutdown")
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not stop")
	}

	code, out := runCLI(context.Background(), map[string]string{"SQS_INPUT_QUEUE": "missing.fifo"}, "serve")
	assert.Equal(t, 1, code, out)
}

// Not parallel: it changes the environment read by the AWS SDK.
func TestCLIProvisionWithBrokenAWSConfig(t *testing.T) {
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	code, _ := runCLI(context.Background(), nil, "provision-queues")
	assert.Equal(t, 1, code)
}
