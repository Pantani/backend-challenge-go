package httpapi_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/httpapi"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

func TestOpenWallet(t *testing.T) {
	t.Parallel()
	var got app.OpenWalletCommand
	w := func(t *testing.T) *wallet.Wallet { return sampleWallet(t) }
	f := &fixture{wallets: fakeWallets{open: func(c app.OpenWalletCommand) (*wallet.Wallet, error) {
		got = c
		return w(t), nil
	}}}
	body := `{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}`
	rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: body})
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, map[string]any{"amount": "1000.00", "currency": "BRL"}, resp["balance"])
	assert.InDelta(t, 1, resp["version"], 0)
	assert.Equal(t, "1000.00", got.InitialBalance.Amount())
	assert.NotEmpty(t, got.CorrelationID)
}

func TestOpenWalletErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{open: func(app.OpenWalletCommand) (*wallet.Wallet, error) { return nil, app.ErrWalletExists }}}
	cases := []struct {
		body   string
		status int
		code   string
	}{
		{`{"playerId":`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"x","unknown":1}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"x"}{}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":1000.00,"currency":"BRL"}}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"NaN","currency":"BRL"}}`, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{`{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1.00","currency":"BRL"}}`, http.StatusConflict, httpapi.CodeWalletExists},
	}
	for _, tc := range cases {
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin", body: tc.body})
		assert.Equal(t, tc.status, rec.Code, tc.body)
		assert.Equal(t, tc.code, resp["code"], tc.body)
	}
}

func TestGetWalletLedgerAndReconciliation(t *testing.T) {
	t.Parallel()
	w := sampleWallet(t)
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{ID: uuid.New(), WalletID: w.ID(), TransactionID: uuid.New(),
		Direction: wallet.Debit, Amount: brl(t, "25.00"), BalanceBefore: brl(t, "1000.00"), BalanceAfter: brl(t, "975.00"), CreatedAt: now})
	require.NoError(t, err)
	var gotCursor string
	var gotLimit int
	f := &fixture{wallets: fakeWallets{
		get: func(uuid.UUID) (*wallet.Wallet, error) { return w, nil },
		ledger: func(_ uuid.UUID, c string, l int) (app.LedgerPage, error) {
			gotCursor, gotLimit = c, l
			return app.LedgerPage{Entries: []wallet.LedgerEntry{entry}, NextCursor: "next"}, nil
		},
		reconcile: func(id uuid.UUID) (app.Reconciliation, error) {
			return app.Reconciliation{WalletID: id, Stored: brl(t, "975.00"), Calculated: brl(t, "975.00"), Difference: brl(t, "0.00"), Consistent: true, CheckedEntries: 2}, nil
		},
	}}
	base := "/wallets/" + w.ID().String()
	rec, resp := f.do(t, call{method: http.MethodGet, path: base, token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, w.ID().String(), resp["id"])

	rec, resp = f.do(t, call{method: http.MethodGet, path: base + "/ledger?cursor=abc&limit=10", token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "next", resp["nextCursor"])
	assert.Len(t, resp["items"], 1)
	assert.Equal(t, "abc", gotCursor)
	assert.Equal(t, 10, gotLimit)

	rec, resp = f.do(t, call{method: http.MethodPost, path: base + "/reconciliation", token: "admin"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, true, resp["consistent"])
	assert.Equal(t, map[string]any{"amount": "0.00", "currency": "BRL"}, resp["difference"])
}

func TestWalletRouteErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wallets: fakeWallets{
		get:       func(uuid.UUID) (*wallet.Wallet, error) { return nil, app.ErrWalletNotFound },
		ledger:    func(uuid.UUID, string, int) (app.LedgerPage, error) { return app.LedgerPage{}, app.ErrUnavailable },
		reconcile: func(uuid.UUID) (app.Reconciliation, error) { return app.Reconciliation{}, fmt.Errorf("db exploded") },
		open: func(app.OpenWalletCommand) (*wallet.Wallet, error) {
			return nil, fmt.Errorf("insert into wallets (secret internals): %w", app.ErrWalletExists)
		},
	}}
	id := uuid.NewString()
	cases := []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/wallets/nope", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id, http.StatusNotFound},
		{http.MethodGet, "/wallets/nope/ledger", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger?limit=x", http.StatusBadRequest},
		{http.MethodGet, "/wallets/" + id + "/ledger", http.StatusServiceUnavailable},
		{http.MethodPost, "/wallets/nope/reconciliation", http.StatusBadRequest},
		{http.MethodPost, "/wallets/" + id + "/reconciliation", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		rec, resp := f.do(t, call{method: tc.method, path: tc.path, token: "admin"})
		assert.Equal(t, tc.status, rec.Code, tc.path)
		assert.NotContains(t, resp["message"], "exploded", "internal errors are not exposed")
	}
	_, resp := f.do(t, call{method: http.MethodPost, path: "/wallets", token: "admin",
		body: `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`})
	assert.Equal(t, "a wallet already exists for this player and currency", resp["message"], "wrapped context never reaches clients")
}

const submitBody = `{"providerId":"provider-a","externalTransactionId":"transaction-123",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",
"roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`

func TestSubmitStatuses(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status wager.Status
		replay bool
		code   int
	}{
		{wager.StatusProcessed, false, http.StatusCreated},
		{wager.StatusProcessed, true, http.StatusOK},
		{wager.StatusPendingReference, false, http.StatusAccepted},
		{wager.StatusRejected, false, http.StatusUnprocessableEntity},
		{wager.StatusFailed, true, http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		tx := sampleTx(t, "provider-a", tc.status)
		var got app.SubmitCommand
		f := &fixture{wagers: fakeWagers{submit: func(c app.SubmitCommand) (app.SubmitResult, error) {
			got = c
			return app.SubmitResult{Transaction: tx, Replay: tc.replay}, nil
		}}}
		rec, resp := f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
			headers: map[string]string{"Idempotency-Key": "provider-a:transaction-123"}})
		assert.Equal(t, tc.code, rec.Code, tc.status)
		assert.Equal(t, string(tc.status), resp["status"])
		assert.Equal(t, tc.replay, resp["idempotentReplay"])
		assert.Equal(t, "provider-a:transaction-123", got.IdempotencyKey)
	}
}

func TestSubmitErrors(t *testing.T) {
	t.Parallel()
	f := &fixture{wagers: fakeWagers{submit: func(app.SubmitCommand) (app.SubmitResult, error) {
		return app.SubmitResult{}, app.ErrIdempotencyConflict
	}}}
	key := map[string]string{"Idempotency-Key": "k"}
	cases := []struct {
		c      call
		status int
		code   string
	}{
		{call{token: "provider-a", body: submitBody}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-b", body: submitBody, headers: key}, http.StatusForbidden, httpapi.CodeForbidden},
		{call{token: "provider-a", body: "[]", headers: key}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-a", body: submitBody[:40] + `"OPENING"}`, headers: key}, http.StatusBadRequest, httpapi.CodeInvalidRequest},
		{call{token: "provider-a", body: submitBody, headers: key}, http.StatusConflict, httpapi.CodeIdempotencyConflict},
	}
	for _, tc := range cases {
		tc.c.method, tc.c.path = http.MethodPost, "/wagering/transactions"
		rec, resp := f.do(t, tc.c)
		assert.Equal(t, tc.status, rec.Code, tc.code)
		assert.Equal(t, tc.code, resp["code"])
	}
}

func TestSubmitUnavailableSetsRetryAfter(t *testing.T) {
	t.Parallel()
	f := &fixture{wagers: fakeWagers{submit: func(app.SubmitCommand) (app.SubmitResult, error) { return app.SubmitResult{}, app.ErrConflict }}}
	rec, resp := f.do(t, call{method: http.MethodPost, path: "/wagering/transactions", token: "provider-a", body: submitBody,
		headers: map[string]string{"Idempotency-Key": "k"}})
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, httpapi.CodeServiceUnavailable, resp["code"])
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestGetTransactionRoutes(t *testing.T) {
	t.Parallel()
	tx := sampleTx(t, "provider-a", wager.StatusPendingReference)
	var callers []app.Caller
	f := &fixture{wagers: fakeWagers{
		get: func(c app.Caller, id uuid.UUID) (*wager.Transaction, error) {
			callers = append(callers, c)
			if id != tx.ID() {
				return nil, app.ErrTransactionNotFound
			}
			return tx, nil
		},
		byExt: func(c app.Caller, p, _ string) (*wager.Transaction, error) {
			callers = append(callers, c)
			if p != c.ProviderID && !c.Internal {
				return nil, app.ErrForbidden
			}
			return tx, nil
		},
	}}
	rec, resp := f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + tx.ID().String(), token: "provider-a"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "PENDING_REFERENCE", resp["status"])
	assert.Equal(t, "2026-09-08T12:00:00.000Z", resp["nextAttemptAt"])
	assert.Equal(t, tx.ReferenceTxID().String(), resp["referenceTransactionId"])
	assert.NotContains(t, resp, "balance", "pending operations have no result balance yet")

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + uuid.NewString(), token: "admin"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	rec, _ = f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/nope", token: "admin"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec, _ = f.do(t, call{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/t-1", token: "admin"})
	assert.Equal(t, http.StatusOK, rec.Code)
	rec, _ = f.do(t, call{method: http.MethodGet, path: "/providers/provider-a/wagering/transactions/t-1", token: "provider-b"})
	assert.Equal(t, http.StatusForbidden, rec.Code)

	assert.Equal(t, app.Caller{ProviderID: "provider-a"}, callers[0])
	assert.Equal(t, app.Caller{Internal: true}, callers[1])
}

func TestTransactionResponseWithoutOptionalFields(t *testing.T) {
	t.Parallel()
	s := wager.Snapshot{ID: uuid.New(), Origin: wager.OriginInternal, Kind: wager.KindOpening, Status: wager.StatusProcessed,
		WalletID: uuid.New(), PlayerID: uuid.New(), Amount: brl(t, "1.00"), ResultBalance: brl(t, "1.00"), CreatedAt: now, UpdatedAt: now}
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	f := &fixture{wagers: fakeWagers{get: func(app.Caller, uuid.UUID) (*wager.Transaction, error) { return tx, nil }}}
	_, resp := f.do(t, call{method: http.MethodGet, path: "/wagering/transactions/" + tx.ID().String(), token: "admin"})
	for _, k := range []string{"providerId", "referenceTransactionId", "nextAttemptAt", "failureCode"} {
		assert.NotContains(t, resp, k)
	}
}
