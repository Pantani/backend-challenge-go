package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Pantani/backend-challenge-go/internal/cli"
)

func run(vars map[string]string, args ...string) (int, string) {
	var out bytes.Buffer
	code := cli.Run(context.Background(), args, func(k string) (string, bool) {
		v, ok := vars[k]
		return v, ok
	}, &out)
	return code, out.String()
}

func TestUsageErrors(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"bogus"}, {"migrate"}} {
		code, out := run(nil, args...)
		assert.Equal(t, 1, code)
		assert.Contains(t, out, cli.ErrUsage.Error())
	}
}

func TestInvalidConfiguration(t *testing.T) {
	t.Parallel()
	code, out := run(map[string]string{"SQS_MAX_MESSAGES": "50"}, "serve")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "invalid configuration")
}

func TestMigrateWithUnreachableDatabase(t *testing.T) {
	t.Parallel()
	code, out := run(map[string]string{"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"}, "migrate", "up")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "error")
}
