package wager

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// brl parses a BRL amount; testutil cannot be imported here (it depends on
// observability, which depends on this package).
func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	require.NoError(t, err)
	return m
}

// Decide only reaches decideMovement for a ROLLBACK once its reference is
// known, so the fail-closed branches of directionOf are exercised directly.
func TestDirectionOfFailsClosed(t *testing.T) {
	t.Parallel()
	_, ok := directionOf(KindRollback, nil)
	assert.False(t, ok, "a ROLLBACK without its reference has no direction")
	_, ok = directionOf(Kind("BOGUS"), nil)
	assert.False(t, ok, "an unknown kind never moves money")
	_, ok = directionOf(KindLoss, nil)
	assert.False(t, ok)
}

func TestDecideMovementRejectsUnknownDirection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	w, _, err := wallet.Open(wallet.OpenParams{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: brl(t, "100.00"),
		OpeningTxID: uuid.New(), OpeningEntryID: uuid.New(), Now: now})
	require.NoError(t, err)
	tx, err := NewExternal(ExternalParams{ID: uuid.New(), WalletID: w.ID(), PlayerID: w.PlayerID(), Kind: KindRollback,
		Amount: brl(t, "10.00"), Now: now, External: External{ProviderID: "p", ExternalID: "e", IdempotencyKey: "k",
			PayloadHash: "h", RoundID: "r", GameID: "g", ReferenceExternalID: "ref"}})
	require.NoError(t, err)
	assert.Equal(t, reject(CodeReferenceKindInvalid), decideMovement(w, tx, nil))
}
