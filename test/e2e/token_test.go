//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/test/testenv"
)

func TestTokenCacheWaitHonorsCallerDeadline(t *testing.T) {
	flowCtx, flowCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer flowCancel()
	started, release := make(chan struct{}), make(chan struct{})
	releaseRequest := sync.OnceFunc(func() { close(release) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		fmt.Fprint(w, `{"access_token":"token"}`)
	}))
	defer srv.Close()
	defer releaseRequest()
	environment := &testenv.Env{KeycloakURL: srv.URL}
	cache := newTokenCache(environment.Token)
	first := make(chan error, 1)
	go func() { _, err := cache.get(flowCtx, "provider-a"); first <- err }()
	require.NoError(t, waitDone(flowCtx, started))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { _, err := cache.get(ctx, "provider-a"); second <- err }()
	var err error
	returned := false
	select {
	case err = <-second:
		returned = true
	case <-time.After(150 * time.Millisecond):
	}
	releaseRequest()
	require.NoError(t, <-first)
	if !returned {
		err = <-second
	}
	require.True(t, returned, "waiting for the refresh lock must respect cancellation")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTokenCacheRefreshHonorsCallerDeadline(t *testing.T) {
	cancelled := make(chan bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			cancelled <- true
		case <-time.After(200 * time.Millisecond):
			cancelled <- false
			fmt.Fprint(w, `{"access_token":"late"}`)
		}
	}))
	defer srv.Close()
	environment := &testenv.Env{KeycloakURL: srv.URL}
	cache := newTokenCache(environment.Token)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := cache.get(ctx, "provider-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, <-cancelled, "the refresh HTTP request must observe cancellation")
}
