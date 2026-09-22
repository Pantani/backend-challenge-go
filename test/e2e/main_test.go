//go:build e2e

// Package e2e_test runs the compiled wallet binary as three independent
// processes (own memory and connections) against real PostgreSQL,
// LocalStack and Keycloak containers, and drives them over HTTP and SQS.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
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
	"github.com/stretchr/testify/require"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/test/testenv"
)

var (
	env       *testenv.Env
	pool      *pgxpool.Pool
	binary    string
	sqsClient sqsadapter.API
	queues    sqsadapter.Queues
	instances []*instance
	tokens    = tokenCache{values: map[string]cachedToken{}}
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if env, err = testenv.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start containers:", err)
		os.Exit(1)
	}
	code := setupAndRun(ctx, m)
	env.Stop(ctx)
	os.Exit(code)
}

func setupAndRun(ctx context.Context, m *testing.M) int {
	if err := prepare(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "prepare:", err)
		return 1
	}
	defer pool.Close()
	defer stopAll()
	return m.Run()
}

// prepare builds the binary with coverage, migrates, provisions the queues
// and starts three instances.
func prepare(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "wallet-e2e")
	if err != nil {
		return err
	}
	binary = filepath.Join(dir, "wallet")
	build := exec.CommandContext(ctx, "go", "build", "-cover", "-covermode=atomic", "-coverpkg=./...", "-o", binary, "./cmd/wallet")
	build.Dir = testenv.RepoRoot()
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
	cmd.Env = os.Environ()
	for k, v := range env.Vars(map[string]string{"LOG_LEVEL": "info", "SQS_VISIBILITY_TIMEOUT": "10s", "SQS_PROCESS_TIMEOUT": "8s"}) {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if dir := os.Getenv("E2E_GOCOVERDIR"); dir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir)
	}
	return cmd
}

// instance is one independent wallet process.
type instance struct {
	name string
	port int
	cmd  *exec.Cmd
	// logs is written by the exec copier goroutine while the process runs.
	logs syncBuffer
}

// syncBuffer is a log sink safe for concurrent writes and reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (i *instance) base() string { return fmt.Sprintf("http://127.0.0.1:%d", i.port) }

func freePort(ctx context.Context) (int, error) {
	l, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func (i *instance) start() error { return i.startWith(context.Background()) }

// startWith launches the process; it is not bound to ctx, which only bounds
// the startup (a test must be able to kill it explicitly).
func (i *instance) startWith(ctx context.Context) error {
	port, err := freePort(ctx)
	if err != nil {
		return err
	}
	i.port = port
	i.cmd = command(context.WithoutCancel(ctx), "serve")
	i.cmd.Env = append(i.cmd.Env, fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", port), "INSTANCE_ID="+i.name)
	i.cmd.Stdout, i.cmd.Stderr = &i.logs, &i.logs
	if err := i.cmd.Start(); err != nil {
		return err
	}
	return i.waitReady(ctx)
}

func (i *instance) waitReady(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if status, _ := get(ctx, i.base()+"/health/ready"); status == http.StatusOK {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("%s not ready: %s", i.name, i.logs.String())
}

func get(ctx context.Context, url string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// kill simulates an abrupt crash (no graceful shutdown).
func (i *instance) kill() {
	_ = i.cmd.Process.Kill()
	_ = i.cmd.Wait()
}

// stop sends SIGTERM and waits for the graceful shutdown.
func (i *instance) stop() error {
	if err := i.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	return i.cmd.Wait()
}

func stopAll() {
	for _, i := range instances {
		_ = i.stop()
	}
}

// tokenRefresh renews cached tokens well before their 5-minute lifetime.
const tokenRefresh = 4 * time.Minute

type cachedToken struct {
	value    string
	issuedAt time.Time
}

type tokenCache struct {
	mu     sync.Mutex
	values map[string]cachedToken
}

// get returns a cached token, fetching a new one when it is about to expire.
func (c *tokenCache) get(client string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tok, ok := c.values[client]; ok && time.Since(tok.issuedAt) < tokenRefresh {
		return tok.value, nil
	}
	value, err := env.Token(context.Background(), client)
	if err != nil {
		return "", err
	}
	c.values[client] = cachedToken{value: value, issuedAt: time.Now()}
	return value, nil
}

type response struct {
	status int
	body   map[string]any
}

// call sends a request to one instance, failing the test on transport errors.
// It must run on the test goroutine; concurrent code uses callE.
func call(t *testing.T, inst *instance, method, path, client, body string, headers map[string]string) response {
	t.Helper()
	res, err := callE(inst, method, path, client, body, headers)
	require.NoError(t, err)
	return res
}

// callE sends a request and returns transport errors instead of failing, so
// it is safe from worker goroutines.
func callE(inst *instance, method, path, client, body string, headers map[string]string) (response, error) {
	tok, err := tokens.get(client)
	if err != nil {
		return response{}, err
	}
	req, err := http.NewRequestWithContext(context.Background(), method, inst.base()+path, strings.NewReader(body))
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	out := response{status: resp.StatusCode}
	return out, json.NewDecoder(resp.Body).Decode(&out.body)
}
