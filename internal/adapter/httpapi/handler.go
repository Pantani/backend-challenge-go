// Package httpapi exposes the use cases over HTTP.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

// WalletUseCases are the internal wallet operations.
type WalletUseCases interface {
	Open(ctx context.Context, cmd app.OpenWalletCommand) (*wallet.Wallet, error)
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	Ledger(ctx context.Context, id uuid.UUID, cursor string, limit int) (app.LedgerPage, error)
	Reconcile(ctx context.Context, id uuid.UUID) (app.Reconciliation, error)
}

// WagerUseCases are the provider operations.
type WagerUseCases interface {
	Submit(ctx context.Context, cmd app.SubmitCommand) (app.SubmitResult, error)
	Get(ctx context.Context, caller app.Caller, id uuid.UUID) (*wager.Transaction, error)
	GetByExternal(ctx context.Context, caller app.Caller, providerID, externalID string) (*wager.Transaction, error)
}

// TokenVerifier validates bearer tokens.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (auth.Principal, error)
}

// HealthCheck probes a dependency for readiness.
type HealthCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// RequestMetrics records served requests.
type RequestMetrics interface {
	HTTPRequest(route string, code int, d time.Duration)
}

// Deps are the collaborators of the HTTP handler.
type Deps struct {
	Wallets        WalletUseCases
	Wagers         WagerUseCases
	Verifier       TokenVerifier
	Checks         []HealthCheck
	Metrics        RequestMetrics
	MetricsHandler http.Handler
	Logger         *slog.Logger
	ReadyTimeout   time.Duration
}

type handler struct {
	Deps
}

// access decides whether an authenticated principal may call a route.
type access func(p auth.Principal) bool

func internalOnly(p auth.Principal) bool       { return p.IsInternal() }
func providerOnly(p auth.Principal) bool       { return p.IsProvider() }
func providerOrInternal(p auth.Principal) bool { return p.IsInternal() || p.IsProvider() }

// NewHandler builds the router. Business routes require a valid bearer token;
// health checks and metrics are public.
func NewHandler(d Deps) http.Handler {
	h := &handler{Deps: d}
	mux := http.NewServeMux()
	h.route(mux, "GET /health/live", h.live)
	h.route(mux, "GET /health/ready", h.ready)
	mux.Handle("GET /metrics", d.MetricsHandler)
	h.route(mux, "POST /wallets", h.secured(internalOnly, h.openWallet))
	h.route(mux, "GET /wallets/{walletId}", h.secured(internalOnly, h.getWallet))
	h.route(mux, "GET /wallets/{walletId}/ledger", h.secured(internalOnly, h.getLedger))
	h.route(mux, "POST /wallets/{walletId}/reconciliation", h.secured(internalOnly, h.reconcile))
	h.route(mux, "POST /wagering/transactions", h.secured(providerOnly, h.submit))
	h.route(mux, "GET /wagering/transactions/{transactionId}", h.secured(providerOrInternal, h.getTransaction))
	h.route(mux, "GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		h.secured(providerOrInternal, h.getTransactionByExternal))
	return mux
}

func (h *handler) route(mux *http.ServeMux, pattern string, next http.HandlerFunc) {
	mux.Handle(pattern, h.observe(pattern, next))
}

// statusRecorder captures the response status for logs and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// observe assigns the correlation id, recovers panics and emits the access
// log and metrics of every request.
func (h *handler) observe(route string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		correlation := correlationID(r)
		w.Header().Set(headerCorrelationID, correlation)
		ctx := observability.WithAttrs(r.Context(), slog.String("correlationId", correlation))
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if p := recover(); p != nil {
				h.Logger.ErrorContext(ctx, "panic serving request", "panic", p)
				writeError(rec, http.StatusInternalServerError, CodeInternalError, "internal error")
			}
			h.Metrics.HTTPRequest(route, rec.status, time.Since(start))
			h.Logger.InfoContext(ctx, "http request", "route", route, "status", rec.status, "durationMs", time.Since(start).Milliseconds())
		}()
		next(rec, r.WithContext(context.WithValue(ctx, correlationKey{}, correlation)))
	})
}

type correlationKey struct{}

func correlationID(r *http.Request) string {
	if id := r.Header.Get(headerCorrelationID); id != "" && len(id) <= 128 {
		return id
	}
	return uuid.NewString()
}

func correlationFrom(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

type principalKey struct{}

func principalFrom(ctx context.Context) auth.Principal {
	p, _ := ctx.Value(principalKey{}).(auth.Principal)
	return p
}

// secured authenticates the bearer token and enforces the route policy.
// Unauthorized calls never reach the use cases, so they have no effect.
func (h *handler) secured(allowed access, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			w.Header().Set(headerWWWAuthenticate, bearerChallenge)
			writeError(w, http.StatusUnauthorized, CodeUnauthorized, "missing bearer token")
			return
		}
		p, err := h.Verifier.Verify(r.Context(), raw)
		if err != nil {
			w.Header().Set(headerWWWAuthenticate, bearerChallengeInvalid)
			writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid or expired token")
			return
		}
		if !allowed(p) {
			writeError(w, http.StatusForbidden, CodeForbidden, "operation not allowed for this client")
			return
		}
		ctx := observability.WithAttrs(r.Context(), slog.String("clientId", p.ClientID), slog.String("providerId", p.ProviderID))
		next(w, r.WithContext(context.WithValue(ctx, principalKey{}, p)))
	}
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code, msg := classify(err)
	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	}
	h.Logger.Log(r.Context(), level, "request failed", "code", code, "error", err)
	writeError(w, status, code, msg)
}

func (h *handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

// ready probes PostgreSQL and SQS; any failure makes the instance unready.
func (h *handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.ReadyTimeout)
	defer cancel()
	status, checks := http.StatusOK, map[string]string{}
	for _, c := range h.Checks {
		checks[c.Name] = "ok"
		if err := c.Check(ctx); err != nil {
			h.Logger.WarnContext(ctx, "readiness check failed", "check", c.Name, "error", err)
			checks[c.Name], status = "unavailable", http.StatusServiceUnavailable
		}
	}
	writeJSON(w, status, map[string]any{"status": readiness(status), "checks": checks})
}

func readiness(status int) string {
	if status == http.StatusOK {
		return "ready"
	}
	return "unready"
}
