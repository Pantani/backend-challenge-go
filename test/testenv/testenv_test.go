//go:build integration || e2e

package testenv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestClientCancellationStopsAuthenticationBeforeAPICall(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan bool, 1)
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			cancelled <- true
		case <-time.After(300 * time.Millisecond):
			cancelled <- false
			fmt.Fprint(w, `{"access_token":"late-token"}`)
		}
	}))
	defer tokenServer.Close()
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{}`)
	}))
	defer api.Close()
	env := Env{KeycloakURL: tokenServer.URL}
	c := Client{Base: api.URL, Token: env.Token}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, http.MethodGet, "/", "provider-a", "", nil)
		result <- err
	}()
	waitCtx, waitCancel := context.WithTimeout(t.Context(), time.Second)
	defer waitCancel()
	_, err := awaitValue(waitCtx, started)
	require.NoError(t, err, "authentication handler did not start")
	cancel()
	requestErr, err := awaitValue(waitCtx, result)
	require.NoError(t, err, "client did not return after cancellation")
	require.ErrorIs(t, requestErr, context.Canceled)
	observed, err := awaitValue(waitCtx, cancelled)
	require.NoError(t, err, "authentication handler did not report cancellation")
	require.True(t, observed, "authentication must observe the caller cancellation")
	require.Zero(t, calls.Load(), "API request must not start after auth cancellation")
}

func TestRepoRootWalksUpToGoMod(t *testing.T) {
	root, err := RepoRoot(filepath.Join("..", "e2e"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoError(t, err)
}

func TestRepoRootFailsOutsideModule(t *testing.T) {
	_, err := RepoRoot(t.TempDir())
	require.ErrorContains(t, err, "go.mod not found")
}

type failingContainer struct {
	testcontainers.Container
	err error
}

func (c failingContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return c.err
}

func TestStopJoinsEveryCleanupFailure(t *testing.T) {
	first, second := errors.New("first cleanup"), errors.New("second cleanup")
	env := Env{containers: []testcontainers.Container{failingContainer{err: first}, failingContainer{err: second}}}
	err := env.Stop(context.Background())
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, second)
}

func TestTokenRejectsHTTPFailureBeforeDecoding(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "invalid credentials provider-a-secret "+strings.Repeat("x", 4096))
	}))
	defer srv.Close()
	env := Env{KeycloakURL: srv.URL}
	_, err := env.Token(context.Background(), "provider-a")
	require.ErrorContains(t, err, "401")
	require.NotContains(t, err.Error(), "provider-a-secret")
	require.Less(t, len(err.Error()), 700)
}

func TestTokenErrorRedactsUnknownCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, `{"error":"invalid_client","access_token":"unknown-sensitive-token","error_description":"unknown-sensitive-secret"}`)
	}))
	defer srv.Close()
	env := Env{KeycloakURL: srv.URL}
	_, err := env.Token(t.Context(), "provider-a")
	require.ErrorContains(t, err, "502")
	require.ErrorContains(t, err, "invalid_client")
	require.NotContains(t, err.Error(), "unknown-sensitive")
}

func TestClientHonorsHTTPTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		fmt.Fprint(w, `{}`)
	}))
	defer srv.Close()
	c := Client{Base: srv.URL, HTTP: http.Client{Timeout: 20 * time.Millisecond}}
	_, err := c.Do(t.Context(), http.MethodGet, "/", "", "", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestClientReturnsMalformedResponseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{`)
	}))
	defer srv.Close()

	res, err := (Client{Base: srv.URL}).Do(t.Context(), http.MethodGet, "/", "", "", nil)
	require.Equal(t, http.StatusOK, res.Status)
	require.Error(t, err)
}

func TestClientRejectsInvalidMethod(t *testing.T) {
	_, err := (Client{Base: "http://example.invalid"}).Do(t.Context(), "invalid\nmethod", "/", "", "", nil)
	require.ErrorContains(t, err, "invalid method")
}

type deadlineTransport struct{}

func (deadlineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	deadline, ok := req.Context().Deadline()
	if !ok {
		return nil, errors.New("HTTP request has no deadline")
	}
	if time.Until(deadline) > 10*time.Second {
		return nil, errors.New("HTTP request deadline exceeds ten seconds")
	}
	return nil, context.DeadlineExceeded
}

func TestClientSuppliesDefaultDeadline(t *testing.T) {
	c := Client{Base: "http://example.invalid", HTTP: http.Client{Transport: deadlineTransport{}}}
	_, err := c.Do(context.Background(), http.MethodGet, "/", "", "", nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAwaitValueHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err := awaitValue(ctx, make(chan bool))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func awaitValue[T any](ctx context.Context, values <-chan T) (T, error) {
	select {
	case value := <-values:
		return value, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
