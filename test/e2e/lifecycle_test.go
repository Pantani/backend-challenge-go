//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func newLifecycleFixture(t *testing.T) *e2eFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	f := &e2eFixture{tempDir: filepath.Join(t.TempDir(), "owned")}
	require.NoError(t, os.Mkdir(f.tempDir, 0o700))
	var err error
	f.pool, err = pgxpool.New(ctx, env.DatabaseURL)
	require.NoError(t, err)
	t.Cleanup(f.pool.Close)
	return f
}

func startLifecycleProcess(t *testing.T, f *e2eFixture) *instance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	i := &instance{name: "lifecycle"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(context.WithoutCancel(ctx), "sh", "-c",
		`trap 'exit 0' TERM; printf '{"msg":"http server listening","addr":"%s"}\n' "$1"; while :; do :; done`,
		"child", strings.TrimPrefix(srv.URL, "http://"))
	require.NoError(t, i.startCommand(ctx, cmd, f))
	done := i.done
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("probe process was not reaped")
		}
	})
	return i
}

func TestFailedRestartPreservesOwnedCleanup(t *testing.T) {
	f := newLifecycleFixture(t)
	first, other := startLifecycleProcess(t, f), startLifecycleProcess(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, first.stop(ctx))
	missing := exec.CommandContext(context.WithoutCancel(ctx), filepath.Join(t.TempDir(), "missing"))
	require.Error(t, first.startCommand(ctx, missing, f))
	require.NotPanics(t, func() { require.NoError(t, f.Close(ctx)) })
	require.NotNil(t, other.cmd.ProcessState)
	require.Error(t, f.pool.Ping(ctx))
	_, err := os.Stat(f.tempDir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestPoolCloseDeadlineDoesNotBlockOtherCleanup(t *testing.T) {
	f := newLifecycleFixture(t)
	i := startLifecycleProcess(t, f)
	acquireCtx, acquireCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer acquireCancel()
	conn, err := f.pool.Acquire(acquireCtx)
	require.NoError(t, err)
	defer conn.Release()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- f.Close(ctx) }()
	var closeErr error
	returned := false
	select {
	case closeErr = <-result:
		returned = true
	case <-time.After(200 * time.Millisecond):
	}
	_, statErr := os.Stat(f.tempDir)
	conn.Release()
	if !returned {
		closeErr = <-result
	}
	require.True(t, returned, "pool close must respect the cleanup deadline")
	require.ErrorIs(t, closeErr, context.DeadlineExceeded)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	reapCtx, reapCancel := context.WithTimeout(t.Context(), time.Second)
	defer reapCancel()
	require.NoError(t, waitDone(reapCtx, i.done))
	require.NotNil(t, i.cmd.ProcessState)
	require.NoError(t, waitDone(reapCtx, f.poolDone), "tracked pool close completes after release")
}

func TestStopDeadlineDoesNotWaitForeverAfterKill(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "sh", "-c", "exit 0")
	require.NoError(t, cmd.Run())
	i := &instance{processRun: &processRun{cmd: cmd, done: make(chan struct{}), killDone: make(chan struct{})}}
	f := &e2eFixture{tempDir: filepath.Join(t.TempDir(), "owned"), instances: []*instance{i}}
	require.NoError(t, os.Mkdir(f.tempDir, 0o700))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- f.Close(ctx) }()
	returned := false
	var err error
	select {
	case err = <-result:
		returned = true
	case <-time.After(150 * time.Millisecond):
	}
	close(i.done)
	if !returned {
		<-result
	}
	require.True(t, returned, "cleanup must not wait for delayed Wait completion after its deadline")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, statErr := os.Stat(f.tempDir)
	require.ErrorIs(t, statErr, os.ErrNotExist, "process wait must not block remaining cleanup")
}

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
	reapCtx, reapCancel := context.WithTimeout(t.Context(), time.Second)
	defer reapCancel()
	require.NoError(t, waitDone(reapCtx, i.done), "the tracked Wait owner must finish after Kill")
	require.NotNil(t, i.cmd.ProcessState, "Wait must reap the process")
	require.True(t, errors.Is(i.cmd.Process.Signal(syscall.Signal(0)), os.ErrProcessDone))
}
