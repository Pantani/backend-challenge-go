//go:build e2e

// Package e2e_test runs the compiled wallet binary as three independent
// processes (own memory and connections) against real PostgreSQL,
// LocalStack and Keycloak containers, and drives them over HTTP and SQS.
package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/semaphore"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

var (
	env       *testenv.Env
	pool      *pgxpool.Pool
	binary    string
	sqsClient sqsadapter.API
	queues    sqsadapter.Queues
	instances []*instance
	tokens    = newTokenCache(func(ctx context.Context, client string) (string, error) { return env.Token(ctx, client) })
	fixture   = &e2eFixture{}
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	var err error
	if env, err = testenv.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		cancel()
		os.Exit(1)
	}
	code := setupAndRun(ctx, m)
	cancel()
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
	if err := errors.Join(fixture.Close(cleanup), env.Stop(cleanup)); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup:", err)
		code = 1
	}
	cleanupCancel()
	os.Exit(code)
}

func setupAndRun(ctx context.Context, m *testing.M) int {
	if err := prepare(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "prepare:", err)
		return 1
	}
	return m.Run()
}

// e2eFixture owns partial setup as well as all successful acquisitions.
type e2eFixture struct {
	tempDir   string
	pool      *pgxpool.Pool
	instances []*instance
	closeOnce sync.Once
	closeErr  error
	poolDone  chan struct{}
}

func (f *e2eFixture) Close(ctx context.Context) error {
	f.closeOnce.Do(func() { f.closeErr = f.closeResources(ctx) })
	return f.closeErr
}

func (f *e2eFixture) closeResources(ctx context.Context) error {
	errs := []error{f.closeProcesses(ctx)}
	if f.pool != nil {
		f.poolDone = make(chan struct{})
		go func() {
			f.pool.Close()
			close(f.poolDone)
		}()
		errs = append(errs, waitDone(ctx, f.poolDone))
	}
	if f.tempDir != "" {
		errs = append(errs, os.RemoveAll(f.tempDir))
	}
	return errors.Join(errs...)
}

func (f *e2eFixture) closeProcesses(ctx context.Context) error {
	_, err := testenv.Parallel(len(f.instances), func(index int) (struct{}, error) {
		return struct{}{}, f.instances[index].stop(ctx)
	})
	return err
}

// prepare builds the binary with coverage, migrates, provisions the queues
// and starts three instances.
func prepare(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "wallet-e2e")
	if err != nil {
		return err
	}
	fixture.tempDir = dir
	binary = filepath.Join(dir, "wallet")
	root, err := testenv.RepoRoot(".")
	if err != nil {
		return err
	}
	build := exec.CommandContext(ctx, "go", "build", "-cover", "-covermode=atomic", "-coverpkg=./...", "-o", binary, "./cmd/wallet")
	build.Dir = root
	build.WaitDelay = time.Second
	if out, err := build.CombinedOutput(); err != nil {
		return fmt.Errorf("build: %w: %s", err, out)
	}
	for _, args := range [][]string{{"migrate", "up"}, {"provision-queues"}} {
		if out, err := command(ctx, args...).CombinedOutput(); err != nil {
			return fmt.Errorf("%v: %w: %s", args, err, out)
		}
	}
	return connect(ctx)
}

func connect(ctx context.Context) error {
	var err error
	if pool, err = pgxpool.New(ctx, env.DatabaseURL); err != nil {
		return err
	}
	fixture.pool = pool
	if sqsClient, err = sqsadapter.NewClient(ctx, sqsadapter.ClientConfig{Region: "us-east-1", Endpoint: env.SQSEndpoint}); err != nil {
		return err
	}
	names := sqsadapter.QueueNames{Input: "wager-transactions.fifo", DLQ: "wager-transactions-dlq.fifo", Events: "wallet-events.fifo"}
	if queues, err = sqsadapter.ResolveQueues(ctx, sqsClient, names); err != nil {
		return err
	}
	for i := range 3 {
		inst := &instance{name: fmt.Sprintf("app-%d", i+1)}
		if err := inst.startWith(ctx); err != nil {
			return err
		}
		instances = append(instances, inst)
	}
	return nil
}

func command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	cmd.Env = os.Environ()
	for k, v := range env.Vars(map[string]string{"LOG_LEVEL": "info", "SQS_VISIBILITY_TIMEOUT": "10s", "SQS_PROCESS_TIMEOUT": "8s", "SQS_ACK_TIMEOUT": "1s"}) {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if dir := os.Getenv("E2E_GOCOVERDIR"); dir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir)
	}
	return cmd
}

// instance is one independent wallet process.
type instance struct {
	*processRun
	name    string
	address string
}

// processRun is immutable ownership of one successfully started execution.
type processRun struct {
	cmd      *exec.Cmd
	done     chan struct{}
	waitErr  error
	stopped  bool
	killOnce sync.Once
	killDone chan struct{}
	killErr  error
	// logs is written by the exec copier goroutine while the process runs.
	logs testutil.SyncBuffer
}

func (i *instance) base() string { return "http://" + i.address }

// client drives this instance with cached Keycloak tokens.
func (i *instance) client() testenv.Client { return testenv.Client{Base: i.base(), Token: tokens.get} }

// startWith launches the process; it is not bound to ctx, which only bounds
// the startup (a test must be able to kill it explicitly).
func (i *instance) startWith(ctx context.Context) error {
	cmd := command(context.WithoutCancel(ctx), "serve")
	cmd.Env = append(cmd.Env, "HTTP_ADDR=127.0.0.1:0", "INSTANCE_ID="+i.name)
	return i.startCommand(ctx, cmd, fixture)
}

func (i *instance) startCommand(ctx context.Context, cmd *exec.Cmd, owner *e2eFixture) error {
	run := &processRun{cmd: cmd, done: make(chan struct{}), killDone: make(chan struct{})}
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = &run.logs, &run.logs
	if err := cmd.Start(); err != nil {
		return err
	}
	i.processRun, i.address = run, ""
	owner.instances = append(owner.instances, &instance{name: i.name, processRun: run})
	go func() {
		run.waitErr = cmd.Wait()
		close(run.done)
	}()
	return i.waitReady(ctx)
}

func (i *instance) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-i.done:
			return errors.Join(fmt.Errorf("%s exited before readiness: %s", i.name, i.logs.String()), i.waitErr)
		case <-ctx.Done():
			return fmt.Errorf("%s readiness: %w: %s", i.name, ctx.Err(), i.logs.String())
		case <-ticker.C:
			if i.ready(ctx) {
				return nil
			}
		}
	}
}

func (i *instance) ready(ctx context.Context) bool {
	if i.address == "" {
		i.address = listeningAddress(i.logs.String())
	}
	if i.address == "" {
		return false
	}
	status, err := get(ctx, i.base()+"/health/ready")
	return err == nil && status == http.StatusOK
}

func listeningAddress(logs string) string {
	for _, line := range strings.Split(logs, "\n") {
		var record struct {
			Message string `json:"msg"`
			Address string `json:"addr"`
		}
		if json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record.Message == "http server listening" {
			return record.Address
		}
	}
	return ""
}

func get(ctx context.Context, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, resp.Body.Close()
}

// kill simulates an abrupt crash and reaps the child before restart.
func (i *instance) kill(ctx context.Context) error {
	killErr := i.processRun.kill(ctx)
	err := waitDone(ctx, i.done)
	i.stopped = err == nil
	return errors.Join(killErr, err)
}

// stop reserves half the available deadline for forced termination and reap.
// The 30-second cap allows the service's 15-second graceful shutdown budget.
func (i *instance) stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if i.stopped {
		return nil
	}
	select {
	case <-i.done:
		i.stopped = true
		return i.waitErr
	default:
	}
	err := i.cmd.Process.Signal(syscall.SIGTERM)
	return errors.Join(processSignalError(err), i.awaitStop(ctx))
}

func (i *instance) awaitStop(ctx context.Context) error {
	deadline, _ := ctx.Deadline() // stop always supplies a bounded context.
	graceful, cancel := context.WithTimeout(ctx, time.Until(deadline)/2)
	defer cancel()
	if err := waitDone(graceful, i.done); err != nil {
		return errors.Join(fmt.Errorf("%s graceful shutdown: %w", i.name, err), i.kill(ctx))
	}
	i.stopped = true
	return i.waitErr
}

func (r *processRun) kill(ctx context.Context) error {
	r.killOnce.Do(func() {
		go func() {
			r.killErr = processSignalError(r.cmd.Process.Kill())
			close(r.killDone)
		}()
	})
	if err := waitDone(ctx, r.killDone); err != nil {
		return err
	}
	return r.killErr
}

func waitDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func processSignalError(err error) error {
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

// tokenRefresh renews cached tokens well before their 5-minute lifetime.
const tokenRefresh = 4 * time.Minute

type cachedToken struct {
	value    string
	issuedAt time.Time
}

type tokenCache struct {
	mu     *semaphore.Weighted
	values map[string]cachedToken
	fetch  func(context.Context, string) (string, error)
}

func newTokenCache(fetch func(context.Context, string) (string, error)) *tokenCache {
	return &tokenCache{mu: semaphore.NewWeighted(1), values: make(map[string]cachedToken), fetch: fetch}
}

// get returns a cached token, fetching a new one when it is about to expire.
func (c *tokenCache) get(ctx context.Context, client string) (string, error) {
	if err := c.mu.Acquire(ctx, 1); err != nil {
		return "", err
	}
	defer c.mu.Release(1)
	if tok, ok := c.values[client]; ok && time.Since(tok.issuedAt) < tokenRefresh {
		return tok.value, nil
	}
	value, err := c.fetch(ctx, client)
	if err != nil {
		return "", err
	}
	c.values[client] = cachedToken{value: value, issuedAt: time.Now()}
	return value, nil
}

// call sends a request to one instance, failing the test on transport errors.
// It must run on the test goroutine; concurrent code uses inst.client().Do.
func call(t *testing.T, inst *instance, method, path, client, body string, headers map[string]string) testenv.Response {
	t.Helper()
	return inst.client().Call(t, method, path, client, body, headers)
}
