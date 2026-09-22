package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/observability"
	"github.com/Pantani/backend-challenge-go/internal/testutil"
)

func TestPanicAfterPartialWriteKeepsResponse(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	h := &handler{Deps: Deps{Metrics: testutil.NewMetrics(), Logger: observability.NewLogger(logs, "debug", "test")}}
	srv := h.observe("GET /boom", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("partial"))
		panic("bug")
	})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/boom", nil))
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "partial", rec.Body.String(), "no JSON error is appended to a started response")
	assert.Contains(t, logs.String(), "panic serving request")
	assert.Contains(t, logs.String(), `"status":202`)
}

func TestStatusRecorderTracksImplicitWriteAndUnwraps(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	sr := &statusRecorder{ResponseWriter: rec, status: http.StatusOK}
	require.False(t, sr.wroteHeader)
	_, err := sr.Write([]byte("x"))
	require.NoError(t, err)
	assert.True(t, sr.wroteHeader)
	assert.Equal(t, http.StatusOK, sr.status)
	sr.WriteHeader(http.StatusTeapot)
	assert.Equal(t, http.StatusOK, sr.status, "the first write fixed the status")
	assert.Same(t, rec, sr.Unwrap())
}
