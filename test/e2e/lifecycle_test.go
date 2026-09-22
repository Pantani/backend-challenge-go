//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestFixtureClosesPoolAndRemovesDirectory(t *testing.T) {
	f := &e2eFixture{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() { require.NoError(t, f.Close(ctx)) })
	var err error
	f.tempDir, err = os.MkdirTemp("", "wallet-e2e-cleanup")
	require.NoError(t, err)
	f.pool, err = pgxpool.New(ctx, env.DatabaseURL)
	require.NoError(t, err)
	require.NoError(t, f.pool.Ping(ctx))
	require.NoError(t, f.Close(ctx))
	require.Error(t, f.pool.Ping(ctx), "closed pool cannot provide connections")
	_, err = os.Stat(f.tempDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, f.Close(ctx))
}

func TestGracefulStopReapsAndIsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	f := &e2eFixture{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	t.Cleanup(func() { require.NoError(t, f.Close(ctx)) })
	i := &instance{name: "graceful"}
	cmd := exec.CommandContext(context.WithoutCancel(ctx), "sh", "-c",
		`trap 'exit 0' TERM; printf '{"msg":"http server listening","addr":"%s"}\n' "$1"; while :; do :; done`,
		"child", strings.TrimPrefix(srv.URL, "http://"))
	require.NoError(t, i.startCommand(ctx, cmd, f))
	require.Equal(t, srv.URL, i.base())
	require.NoError(t, f.Close(ctx))
	require.True(t, i.cmd.ProcessState.Success())
	require.NoError(t, f.Close(ctx))
}

func TestFixtureOwnsProcessBeforeReadinessFailure(t *testing.T) {
	f := &e2eFixture{}
	dir, err := os.MkdirTemp("", "wallet-e2e-lifecycle")
	require.NoError(t, err)
	f.tempDir = dir
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = f.Close(ctx) // The expected failure is asserted below.
	})
	i := &instance{name: "early-exit"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = i.startCommand(ctx, exec.CommandContext(context.WithoutCancel(ctx), "sh", "-c", "exit 7"), f)
	require.ErrorContains(t, err, "exited before readiness")
	require.Len(t, f.instances, 1)
	require.Error(t, f.Close(ctx), "unexpected process exit must fail cleanup")
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
	// Repeated cleanup returns the same outcome without touching resources again.
	require.Error(t, f.Close(ctx))
}

func TestProcessTimeoutKillsAndReapsChild(t *testing.T) {
	f := &e2eFixture{}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = f.Close(ctx) // The expected deadline failure is asserted below.
	})
	i := &instance{name: "ignores-term"}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	err := i.startCommand(ctx, exec.CommandContext(context.WithoutCancel(ctx), "sh", "-c", "trap '' TERM; while :; do :; done"), f)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cleanup, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
	defer cleanupCancel()
	err = f.Close(cleanup)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, i.cmd.ProcessState, "Wait must reap the process")
	require.True(t, errors.Is(i.cmd.Process.Signal(syscall.Signal(0)), os.ErrProcessDone))
}
