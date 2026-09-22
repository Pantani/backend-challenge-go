//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	sqsadapter "github.com/Pantani/backend-challenge-go/internal/adapter/sqs"
	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/worker"
)

type runningApp struct {
	base   string
	group  *worker.Group
	app    *fx.App
	api    sqsadapter.API
	queues sqsadapter.Queues
}

func queueVars(names sqsadapter.QueueNames) map[string]string {
	return map[string]string{"SQS_INPUT_QUEUE": names.Input, "SQS_DLQ": names.DLQ, "SQS_EVENTS_QUEUE": names.Events}
}

func newApp(t *testing.T, overrides map[string]string) (*fx.App, *bootstrap.Addr, *worker.Group) {
	t.Helper()
	cfg, err := env.Config(overrides)
	require.NoError(t, err)
	var addr *bootstrap.Addr
	var group *worker.Group
	a := bootstrap.New(cfg, fx.Replace(bootstrap.LogOutput{Writer: io.Discard}), fx.Populate(&addr, &group))
	return a, addr, group
}

func startApp(t *testing.T) runningApp {
	t.Helper()
	api, q, names := provisionQueues(t, 3)
	a, addr, group := newApp(t, queueVars(names))
	require.NoError(t, a.Start(context.Background()))
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	return runningApp{base: "http://" + addr.String(), group: group, app: a, api: api, queues: q}
}

type response struct {
	status int
	body   map[string]any
}

func (r runningApp) call(t *testing.T, method, path, client, body string, headers map[string]string) response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, r.base+path, strings.NewReader(body))
	require.NoError(t, err)
	if client != "" {
		req.Header.Set("Authorization", "Bearer "+token(t, client))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out := response{status: resp.StatusCode}
	_ = json.NewDecoder(resp.Body).Decode(&out.body)
	return out
}

func (r runningApp) openWallet(t *testing.T, amount string) string {
	t.Helper()
	res := r.call(t, http.MethodPost, "/wallets", "wallet-service",
		`{"playerId":"`+uuid.NewString()+`","initialBalance":{"amount":"`+amount+`","currency":"BRL"}}`, nil)
	require.Equal(t, http.StatusCreated, res.status, res.body)
	return res.body["id"].(string)
}

func (r runningApp) wallet(t *testing.T, id string) map[string]any {
	t.Helper()
	res := r.call(t, http.MethodGet, "/wallets/"+id, "wallet-service", "", nil)
	require.Equal(t, http.StatusOK, res.status)
	return res.body
}

func betBody(provider, ext, walletID, playerID, amount string) string {
	return `{"providerId":"` + provider + `","externalTransactionId":"` + ext + `","playerId":"` + playerID +
		`","walletId":"` + walletID + `","roundId":"r-1","gameId":"g-1","kind":"BET","money":{"amount":"` + amount + `","currency":"BRL"}}`
}

func TestApplicationServesAuthenticatedFlows(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	assert.Equal(t, http.StatusOK, r.call(t, http.MethodGet, "/health/live", "", "", nil).status)
	assert.Equal(t, http.StatusOK, r.call(t, http.MethodGet, "/health/ready", "", "", nil).status)

	walletID := r.openWallet(t, "1000.00")
	player := r.wallet(t, walletID)["playerId"].(string)
	ext := uuid.NewString()
	key := map[string]string{"Idempotency-Key": "provider-a:" + ext}
	bet := r.call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody("provider-a", ext, walletID, player, "25.00"), key)
	require.Equal(t, http.StatusCreated, bet.status, bet.body)
	assert.Equal(t, map[string]any{"amount": "975.00", "currency": "BRL"}, bet.body["balance"])

	replay := r.call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody("provider-a", ext, walletID, player, "25.00"), key)
	assert.Equal(t, http.StatusOK, replay.status)
	assert.Equal(t, true, replay.body["idempotentReplay"])
	conflict := r.call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody("provider-a", ext, walletID, player, "26.00"), key)
	assert.Equal(t, http.StatusConflict, conflict.status)

	txID := bet.body["transactionId"].(string)
	assert.Equal(t, http.StatusOK, r.call(t, http.MethodGet, "/wagering/transactions/"+txID, "provider-a", "", nil).status)
	assert.Equal(t, http.StatusOK, r.call(t, http.MethodGet, "/providers/provider-a/wagering/transactions/"+ext, "provider-a", "", nil).status)
	ledger := r.call(t, http.MethodGet, "/wallets/"+walletID+"/ledger?limit=1", "wallet-service", "", nil)
	assert.Len(t, ledger.body["items"], 1)
	assert.NotEmpty(t, ledger.body["nextCursor"])
	rec := r.call(t, http.MethodPost, "/wallets/"+walletID+"/reconciliation", "wallet-service", "", nil)
	assert.Equal(t, true, rec.body["consistent"])
	assert.InDelta(t, 2, rec.body["checkedEntries"], 0)
}

func TestProviderIsolationAndNoEffectsWhenUnauthorized(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	walletID := r.openWallet(t, "100.00")
	player := r.wallet(t, walletID)["playerId"].(string)
	ext := uuid.NewString()
	key := map[string]string{"Idempotency-Key": "provider-a:" + ext}
	bet := r.call(t, http.MethodPost, "/wagering/transactions", "provider-a", betBody("provider-a", ext, walletID, player, "10.00"), key)
	require.Equal(t, http.StatusCreated, bet.status)
	txID := bet.body["transactionId"].(string)

	cases := []struct {
		method, path, client, body string
		want                       int
	}{
		{http.MethodGet, "/wagering/transactions/" + txID, "provider-b", "", http.StatusNotFound},
		{http.MethodGet, "/providers/provider-a/wagering/transactions/" + ext, "provider-b", "", http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "provider-b", betBody("provider-a", ext, walletID, player, "10.00"), http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "provider-a", betBody("provider-a", uuid.NewString(), walletID, player, "50.00"), http.StatusBadRequest},
		{http.MethodPost, "/wagering/transactions", "", betBody("provider-a", uuid.NewString(), walletID, player, "50.00"), http.StatusUnauthorized},
		{http.MethodPost, "/wagering/transactions", "no-role-client", betBody("provider-a", uuid.NewString(), walletID, player, "50.00"), http.StatusForbidden},
		{http.MethodPost, "/wagering/transactions", "wallet-service", betBody("provider-a", uuid.NewString(), walletID, player, "50.00"), http.StatusForbidden},
		{http.MethodPost, "/wallets", "provider-a", `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}}`, http.StatusForbidden},
		{http.MethodPost, "/wallets/" + walletID + "/reconciliation", "provider-a", "", http.StatusForbidden},
		{http.MethodGet, "/wallets/" + walletID + "/ledger", "provider-b", "", http.StatusForbidden},
	}
	for _, c := range cases {
		headers := map[string]string{"Idempotency-Key": "provider-a:" + ext}
		if c.want == http.StatusBadRequest {
			headers = nil // missing Idempotency-Key
		}
		res := r.call(t, c.method, c.path, c.client, c.body, headers)
		assert.Equal(t, c.want, res.status, "%s %s as %s", c.method, c.path, c.client)
		assert.NotContains(t, res.body, "transactionId", "no data is exposed")
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, r.base+"/wallets/"+walletID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer forged.token.value")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	w := r.wallet(t, walletID)
	assert.Equal(t, map[string]any{"amount": "90.00", "currency": "BRL"}, w["balance"], "no financial effect")
	assert.InDelta(t, 2, w["version"], 0)
}

func TestApplicationConsumesSQSAndPublishesEvents(t *testing.T) {
	t.Parallel()
	r := startApp(t)
	walletID := r.openWallet(t, "100.00")
	player := r.wallet(t, walletID)["playerId"].(string)
	ext := uuid.NewString()
	sendMessage(t, r.api, r.queues, "msg-"+ext, app.SubmitInput{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext, PlayerID: player,
		WalletID: walletID, RoundID: "r-1", GameID: "g-1", Kind: "BET", Amount: "15.00", Currency: "BRL",
	})

	require.Eventually(t, func() bool {
		return r.wallet(t, walletID)["balance"].(map[string]any)["amount"] == "85.00"
	}, 20*time.Second, 200*time.Millisecond)
	// Any running instance may publish them (the outbox is shared), so the
	// publication is checked on the outbox itself.
	wid, err := uuid.Parse(walletID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return unpublished(t, wid) == 0 }, 20*time.Second, 200*time.Millisecond,
		"opening and bet events published after commit")
}

func TestApplicationStopsWorkersOnShutdown(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	a, addr, group := newApp(t, queueVars(names))
	require.NoError(t, a.Start(context.Background()))
	require.Positive(t, group.Running())
	base := "http://" + addr.String()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	require.NoError(t, a.Stop(ctx))
	assert.Zero(t, group.Running(), "every worker returned")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/health/live", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err, "no new inputs after shutdown")
}

func TestApplicationRefusesToStartWithBrokenDependencies(t *testing.T) {
	t.Parallel()
	_, _, names := provisionQueues(t, 3)
	cases := map[string]map[string]string{
		"missing queue": {"SQS_INPUT_QUEUE": "does-not-exist.fifo"},
		"database down": {"DATABASE_URL": "postgres://wallet:wallet@127.0.0.1:1/wallet?sslmode=disable"},
		"bad database":  {"DATABASE_URL": "postgres://%%%"},
		"bad listen":    {"HTTP_ADDR": "256.0.0.1:1"},
		"sender policy": {"SQS_SENDER_PROVIDERS": "missing-equals"},
	}
	for name, overrides := range cases {
		vars := queueVars(names)
		for k, v := range overrides {
			vars[k] = v
		}
		a, _, _ := newApp(t, vars)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := a.Start(ctx)
		cancel()
		assert.Error(t, err, name)
	}
}

func TestFxGraphIsValid(t *testing.T) {
	t.Parallel()
	cfg, err := env.Config(nil)
	require.NoError(t, err)
	require.NoError(t, fx.ValidateApp(bootstrap.Options(cfg)))
}

// Not parallel: it changes the process environment read by the AWS SDK.
func TestApplicationRefusesToStartWithBrokenAWSConfig(t *testing.T) {
	t.Setenv("AWS_PROFILE", "profile-that-does-not-exist")
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/missing")
	a, _, _ := newApp(t, nil)
	require.Error(t, a.Err())
}
