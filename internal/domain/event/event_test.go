package event_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 123456789, time.FixedZone("BRT", -3*3600))

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	require.NoError(t, err)
	return m
}

func externalTx(t *testing.T, kind wager.Kind, amount, ref string) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(wager.ExternalParams{
		ID: uuid.New(), WalletID: uuid.New(), PlayerID: uuid.New(), Kind: kind, Amount: brl(t, amount),
		CorrelationID: "corr", Now: now,
		External: wager.External{ProviderID: "provider-a", ExternalID: "t-1", IdempotencyKey: "k", PayloadHash: "h",
			RoundID: "r-1", GameID: "g-1", ReferenceExternalID: ref},
	})
	require.NoError(t, err)
	return tx
}

func decode(t *testing.T, r event.Record) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(r.Payload, &out))
	return out
}

func meta() event.Meta {
	return event.Meta{EventID: uuid.New(), CorrelationID: "corr", CausationID: "msg-1", OccurredAt: now}
}

func TestProcessedEnvelope(t *testing.T) {
	t.Parallel()
	tx := externalTx(t, wager.KindRefund, "25.00", "t-0")
	refID := uuid.New()
	require.NoError(t, tx.Process(brl(t, "975.00"), refID, now))
	m := meta()

	r := event.NewWagerTransactionProcessed(m, tx)
	assert.Equal(t, m.EventID, r.EventID)
	assert.Equal(t, event.TypeWagerTransactionProcessed, r.EventType)
	assert.Equal(t, event.AggregateTransaction, r.AggregateType)
	assert.Equal(t, tx.ID(), r.AggregateID)
	assert.Equal(t, tx.WalletID().String(), r.PartitionKey)
	assert.Equal(t, now.UTC(), r.OccurredAt)

	doc := decode(t, r)
	assert.Equal(t, m.EventID.String(), doc["eventId"])
	assert.Equal(t, "msg-1", doc["causationId"])
	assert.Equal(t, "corr", doc["correlationId"])
	assert.Equal(t, "2026-09-08T15:00:00.123Z", doc["occurredAt"])
	assert.InDelta(t, event.SchemaVersion, doc["version"], 0)
	data := doc["data"].(map[string]any)
	assert.Equal(t, map[string]any{"amount": "25.00", "currency": "BRL"}, data["money"])
	assert.Equal(t, map[string]any{"amount": "975.00", "currency": "BRL"}, data["balance"])
	assert.Equal(t, refID.String(), data["referenceTransactionId"])
	assert.Equal(t, "t-0", data["referenceExternalTransactionId"])
	assert.Equal(t, "PROCESSED", data["status"])
}

func TestOpeningProcessedOmitsExternalFields(t *testing.T) {
	t.Parallel()
	tx, err := wager.NewOpening(wager.OpeningParams{ID: uuid.New(), WalletID: uuid.New(), PlayerID: uuid.New(), Amount: brl(t, "10.00"), Now: now})
	require.NoError(t, err)
	require.NoError(t, tx.Process(brl(t, "10.00"), uuid.Nil, now))
	m := meta()
	m.CausationID = ""

	doc := decode(t, event.NewWagerTransactionProcessed(m, tx))
	assert.NotContains(t, doc, "causationId")
	data := doc["data"].(map[string]any)
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "referenceTransactionId"} {
		assert.NotContains(t, data, k)
	}
	assert.Equal(t, "OPENING", data["kind"])
	assert.Equal(t, "INTERNAL", data["origin"])
}

func TestRejectedAndPendingEvents(t *testing.T) {
	t.Parallel()
	rejected := externalTx(t, wager.KindBet, "80.00", "")
	require.NoError(t, rejected.Reject(wager.CodeInsufficientFunds, brl(t, "20.00"), now))
	doc := decode(t, event.NewWagerTransactionRejected(meta(), rejected))
	assert.Equal(t, event.TypeWagerTransactionRejected, doc["eventType"])
	assert.Equal(t, "INSUFFICIENT_FUNDS", doc["data"].(map[string]any)["failureCode"])

	pending := externalTx(t, wager.KindRollback, "80.00", "t-0")
	require.NoError(t, pending.AwaitReference(now.Add(time.Second), now))
	doc = decode(t, event.NewWagerTransactionPendingReference(meta(), pending))
	assert.Equal(t, event.TypeWagerTransactionPendingReference, doc["eventType"])
	assert.Equal(t, "2026-09-08T15:00:01.123Z", doc["data"].(map[string]any)["nextAttemptAt"])
}

func TestWalletBalanceChanged(t *testing.T) {
	t.Parallel()
	entry, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit,
		Amount: brl(t, "25.00"), BalanceBefore: brl(t, "1000.00"), BalanceAfter: brl(t, "975.00"), CreatedAt: now,
	})
	require.NoError(t, err)
	r := event.NewWalletBalanceChanged(meta(), entry, 2)
	assert.Equal(t, event.AggregateWallet, r.AggregateType)
	assert.Equal(t, entry.WalletID(), r.AggregateID)

	data := decode(t, r)["data"].(map[string]any)
	assert.Equal(t, entry.WalletID().String(), data["walletId"])
	assert.Equal(t, entry.TransactionID().String(), data["transactionId"])
	assert.Equal(t, "DEBIT", data["direction"])
	assert.Equal(t, map[string]any{"amount": "1000.00", "currency": "BRL"}, data["balanceBefore"])
	assert.Equal(t, map[string]any{"amount": "975.00", "currency": "BRL"}, data["balanceAfter"])
	assert.InDelta(t, 2, data["walletVersion"], 0)
}
