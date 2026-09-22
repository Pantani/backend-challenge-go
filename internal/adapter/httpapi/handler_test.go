package httpapi_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type fakeWallets struct {
	open      func(app.OpenWalletCommand) (*wallet.Wallet, error)
	get       func(uuid.UUID) (*wallet.Wallet, error)
	ledger    func(uuid.UUID, string, int) (app.LedgerPage, error)
	reconcile func(uuid.UUID) (app.Reconciliation, error)
}

func (f fakeWallets) Open(_ context.Context, c app.OpenWalletCommand) (*wallet.Wallet, error) {
	return f.open(c)
}
func (f fakeWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) { return f.get(id) }
func (f fakeWallets) Ledger(_ context.Context, id uuid.UUID, c string, l int) (app.LedgerPage, error) {
	return f.ledger(id, c, l)
}
func (f fakeWallets) Reconcile(_ context.Context, id uuid.UUID) (app.Reconciliation, error) {
	return f.reconcile(id)
}

type fakeWagers struct {
	submit func(app.SubmitCommand) (app.SubmitResult, error)
	get    func(app.Caller, uuid.UUID) (*wager.Transaction, error)
	byExt  func(app.Caller, string, string) (*wager.Transaction, error)
}

func (f fakeWagers) Submit(_ context.Context, c app.SubmitCommand) (app.SubmitResult, error) {
	return f.submit(c)
}
func (f fakeWagers) Get(_ context.Context, c app.Caller, id uuid.UUID) (*wager.Transaction, error) {
	return f.get(c, id)
}
func (f fakeWagers) GetByExternal(_ context.Context, c app.Caller, p, e string) (*wager.Transaction, error) {
	return f.byExt(c, p, e)
}

// tokens maps bearer tokens to principals; unknown tokens are invalid.
type tokens map[string]auth.Principal

func (t tokens) Verify(_ context.Context, raw string) (auth.Principal, error) {
	p, ok := t[raw]
	if !ok {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	return p, nil
}

var principals = tokens{
	"admin":      {ClientID: "wallet-service", Roles: []string{auth.RoleWalletAdmin}},
	"provider-a": {ClientID: "provider-a", ProviderID: "provider-a", Roles: []string{auth.RoleProvider}},
	"provider-b": {ClientID: "provider-b", ProviderID: "provider-b", Roles: []string{auth.RoleProvider}},
	"no-roles":   {ClientID: "nobody"},
}

type fixture struct {
	wallets fakeWallets
	wagers  fakeWagers
	checks  []httpapi.HealthCheck
	logs    *bytes.Buffer
}

func (f *fixture) server() http.Handler {
	f.logs = &bytes.Buffer{}
	return httpapi.NewHandler(httpapi.Deps{
		Wallets: f.wallets, Wagers: f.wagers, Verifier: principals, Checks: f.checks,
		Metrics: observability.NewMetrics(prometheus.NewRegistry()), MetricsHandler: http.NotFoundHandler(),
		Logger: observability.NewLogger(f.logs, "debug", "test"), ReadyTimeout: time.Second,
	})
}

type call struct {
	method, path, token, body string
	scheme                    string
	headers                   map[string]string
}

func (f *fixture) do(t *testing.T, c call) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), c.method, c.path, strings.NewReader(c.body))
	if c.token != "" {
		req.Header.Set("Authorization", cmp.Or(c.scheme, "Bearer")+" "+c.token)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	f.server().ServeHTTP(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec, body
}

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	require.NoError(t, err)
	return m
}

func sampleWallet(t *testing.T) *wallet.Wallet {
	t.Helper()
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, "1000.00"), Version: 1, CreatedAt: now, UpdatedAt: now})
	require.NoError(t, err)
	return w
}

func sampleTx(t *testing.T, provider string, status wager.Status) *wager.Transaction {
	t.Helper()
	s := wager.Snapshot{
		ID: uuid.New(), Origin: wager.OriginExternal, Kind: wager.KindRefund, Status: status, WalletID: uuid.New(),
		PlayerID: uuid.New(), Amount: brl(t, "25.00"), ResultBalance: brl(t, "975.00"), ReferenceTxID: uuid.New(),
		External: wager.External{ProviderID: provider, ExternalID: "t-1", IdempotencyKey: "k", PayloadHash: "h",
			RoundID: "r", GameID: "g", ReferenceExternalID: "t-0"},
		NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if status == wager.StatusRejected {
		s.FailureCode = wager.CodeInsufficientFunds
	}
	if status == wager.StatusPendingReference {
		s.ResultBalance = money.Money{}
	}
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	return tx
}

func TestHealth(t *testing.T) {
	t.Parallel()
	f := &fixture{checks: []httpapi.HealthCheck{
		{Name: "postgres", Check: func(context.Context) error { return nil }},
	}}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/health/live"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "alive", body["status"])

	rec, body = f.do(t, call{method: http.MethodGet, path: "/health/ready"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ready", body["status"])

	f.checks = append(f.checks, httpapi.HealthCheck{Name: "sqs", Check: func(context.Context) error { return errors.New("down") }})
	rec, body = f.do(t, call{method: http.MethodGet, path: "/health/ready"})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, map[string]any{"postgres": "ok", "sqs": "unavailable"}, body["checks"])
}

func TestAuthentication(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString()})
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Equal(t, httpapi.CodeUnauthorized, body["code"])
	assert.Contains(t, rec.Header().Get("WWW-Authenticate"), "Bearer")

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "forged"})
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
	assert.Contains(t, rec.Header().Get("WWW-Authenticate"), "invalid_token")

	for _, scheme := range []string{"Basic", "Bearer\t"} {
		rec, _ = f.do(t, call{method: http.MethodGet, path: "/health/ready", token: "admin", scheme: scheme})
		assert.Equal(t, http.StatusOK, rec.Code, "public route")
		rec, _ = f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", scheme: scheme})
		assert.Equal(t, http.StatusUnauthorized, rec.Code, scheme)
	}
}

func TestBearerSchemeIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{get: func(uuid.UUID) (*wallet.Wallet, error) { return sampleWallet(t), nil }}}
	for _, scheme := range []string{"bearer", "BEARER", "Bearer "} {
		rec, _ := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "admin", scheme: scheme})
		assert.Equal(t, http.StatusOK, rec.Code, scheme)
	}
}

func TestAuthorizationPolicies(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	id := uuid.NewString()
	forbidden := []call{
		{method: http.MethodPost, path: "/wallets", token: "provider-a", body: "{}"},
		{method: http.MethodGet, path: "/wallets/" + id, token: "provider-a"},
		{method: http.MethodGet, path: "/wallets/" + id + "/ledger", token: "provider-a"},
		{method: http.MethodPost, path: "/wallets/" + id + "/reconciliation", token: "provider-a"},
		{method: http.MethodPost, path: "/wagering/transactions", token: "admin", body: "{}"},
		{method: http.MethodGet, path: "/wagering/transactions/" + id, token: "no-roles"},
	}
	for _, c := range forbidden {
		rec, body := f.do(t, c)
		assert.Equal(t, http.StatusForbidden, rec.Code, c.path)
		assert.Equal(t, httpapi.CodeForbidden, body["code"])
	}
}

func TestPanicRecovery(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{get: func(uuid.UUID) (*wallet.Wallet, error) { panic("bug") }}}
	rec, body := f.do(t, call{method: http.MethodGet, path: "/wallets/" + uuid.NewString(), token: "admin"})
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Equal(t, httpapi.CodeInternalError, body["code"])
	assert.Contains(t, f.logs.String(), "panic serving request")
}

func TestCorrelationID(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, _ := f.do(t, call{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-Id": "abc"}})
	assert.Equal(t, "abc", rec.Header().Get("X-Correlation-Id"))
	rec, _ = f.do(t, call{method: http.MethodGet, path: "/health/live", headers: map[string]string{"X-Correlation-Id": strings.Repeat("x", 200)}})
	assert.Len(t, rec.Header().Get("X-Correlation-Id"), 36)
	assert.Contains(t, f.logs.String(), `"correlationId"`)
}

func TestMetricsRoute(t *testing.T) {
	t.Parallel()
	f := &fixture{}
	rec, _ := f.do(t, call{method: http.MethodGet, path: "/metrics"})
	assert.Equal(t, http.StatusNotFound, rec.Code, "served by the injected handler")
}
