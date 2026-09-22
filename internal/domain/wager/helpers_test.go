package wager_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func mny(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	require.NoError(t, err)
	return m
}

type fixture struct {
	wallet   *wallet.Wallet
	walletID uuid.UUID
	playerID uuid.UUID
}

func newFixture(t *testing.T, balance string) fixture {
	t.Helper()
	w, _, err := wallet.Open(wallet.OpenParams{
		ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: mny(t, balance, "BRL"),
		OpeningTxID: uuid.New(), OpeningEntryID: uuid.New(), Now: now,
	})
	require.NoError(t, err)
	return fixture{wallet: w, walletID: w.ID(), playerID: w.PlayerID()}
}

func (f fixture) params(t *testing.T, kind wager.Kind, amount, ref string) wager.ExternalParams {
	t.Helper()
	return wager.ExternalParams{
		ID: uuid.New(), WalletID: f.walletID, PlayerID: f.playerID, Kind: kind,
		Amount: mny(t, amount, "BRL"), CorrelationID: "corr-1", Now: now,
		External: wager.External{
			ProviderID: "provider-a", ExternalID: "ext-" + uuid.NewString(), IdempotencyKey: "key-" + uuid.NewString(),
			PayloadHash: "hash", RoundID: "round-1", GameID: "game-1", ReferenceExternalID: ref,
		},
	}
}

func (f fixture) tx(t *testing.T, kind wager.Kind, amount, ref string) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(f.params(t, kind, amount, ref))
	require.NoError(t, err)
	return tx
}

// processed returns a PROCESSED transaction of the given kind.
func (f fixture) processed(t *testing.T, kind wager.Kind, amount, ref string) *wager.Transaction {
	t.Helper()
	tx := f.tx(t, kind, amount, ref)
	require.NoError(t, tx.Process(f.wallet.Balance(), uuid.Nil, now))
	return tx
}
