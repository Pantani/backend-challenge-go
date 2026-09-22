package wager_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

func TestParseExternalKind(t *testing.T) {
	t.Parallel()
	for _, k := range []string{"BET", "WIN", "LOSS", "REFUND", "ROLLBACK"} {
		got, err := wager.ParseExternalKind(k)
		require.NoError(t, err)
		assert.Equal(t, wager.Kind(k), got)
	}
	for _, k := range []string{"OPENING", "bet", "", "DEPOSIT"} {
		_, err := wager.ParseExternalKind(k)
		assert.ErrorIs(t, err, wager.ErrInvalidKind, k)
	}
}

func TestKindPredicates(t *testing.T) {
	t.Parallel()
	assert.True(t, wager.KindOpening.Valid())
	assert.False(t, wager.Kind("X").Valid())
	assert.True(t, wager.KindRefund.RequiresReference())
	assert.True(t, wager.KindRollback.IsReversal())
	assert.False(t, wager.KindWin.RequiresReference())
	assert.True(t, wager.KindWin.AcceptsReference())
	assert.False(t, wager.KindBet.AcceptsReference())
	assert.True(t, wager.KindLoss.RequiresZeroAmount())
}

func TestStatusPredicates(t *testing.T) {
	t.Parallel()
	terminal := map[wager.Status]bool{
		wager.StatusPending: false, wager.StatusPendingReference: false,
		wager.StatusProcessed: true, wager.StatusRejected: true, wager.StatusFailed: true,
	}
	for s, want := range terminal {
		assert.True(t, s.Valid())
		assert.Equal(t, want, s.Terminal(), s)
	}
	assert.False(t, wager.Status("X").Valid())
}

func TestZeroValuePolicy(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	cases := []struct {
		kind   wager.Kind
		amount string
		ref    string
		ok     bool
	}{
		{wager.KindBet, "1.00", "", true},
		{wager.KindBet, "0.00", "", false},
		{wager.KindWin, "1.00", "", true},
		{wager.KindWin, "0.00", "", false},
		{wager.KindLoss, "0.00", "", true},
		{wager.KindLoss, "1.00", "", false},
		{wager.KindRefund, "1.00", "bet-1", true},
		{wager.KindRefund, "0.00", "bet-1", false},
		{wager.KindRollback, "1.00", "bet-1", true},
		{wager.KindRollback, "0.00", "bet-1", false},
	}
	for _, tc := range cases {
		_, err := wager.NewExternal(f.params(t, tc.kind, tc.amount, tc.ref))
		assert.Equal(t, tc.ok, err == nil, "%s %s: %v", tc.kind, tc.amount, err)
	}
}

func TestNewExternal(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	p := f.params(t, wager.KindRefund, "25.00", "bet-1")
	tx, err := wager.NewExternal(p)
	require.NoError(t, err)

	assert.Equal(t, p.ID, tx.ID())
	assert.Equal(t, wager.OriginExternal, tx.Origin())
	assert.Equal(t, wager.KindRefund, tx.Kind())
	assert.Equal(t, wager.StatusPending, tx.Status())
	assert.Equal(t, f.walletID, tx.WalletID())
	assert.Equal(t, f.playerID, tx.PlayerID())
	assert.Equal(t, "25.00", tx.Amount().Amount())
	assert.Equal(t, p.External, tx.External())
	assert.Equal(t, "corr-1", tx.CorrelationID())
	assert.Equal(t, now, tx.CreatedAt())
	assert.Equal(t, now, tx.UpdatedAt())
	assert.Equal(t, uuid.Nil, tx.ReferenceTxID())
	assert.Empty(t, tx.FailureCode())
	assert.Zero(t, tx.Attempts())
	assert.True(t, tx.NextAttemptAt().IsZero())
	assert.Error(t, tx.ResultBalance().Validate())
}

func TestNewExternalValidation(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	long := strings.Repeat("x", 129)
	mutations := map[string]func(p *wager.ExternalParams){
		"opening kind":          func(p *wager.ExternalParams) { p.Kind = wager.KindOpening },
		"nil id":                func(p *wager.ExternalParams) { p.ID = uuid.Nil },
		"nil wallet":            func(p *wager.ExternalParams) { p.WalletID = uuid.Nil },
		"nil player":            func(p *wager.ExternalParams) { p.PlayerID = uuid.Nil },
		"zero time":             func(p *wager.ExternalParams) { p.Now = time.Time{} },
		"uninitialized money":   func(p *wager.ExternalParams) { p.Amount = money.Money{} },
		"empty provider":        func(p *wager.ExternalParams) { p.External.ProviderID = "" },
		"empty external id":     func(p *wager.ExternalParams) { p.External.ExternalID = "" },
		"empty key":             func(p *wager.ExternalParams) { p.External.IdempotencyKey = "" },
		"empty hash":            func(p *wager.ExternalParams) { p.External.PayloadHash = "" },
		"empty round":           func(p *wager.ExternalParams) { p.External.RoundID = "" },
		"empty game":            func(p *wager.ExternalParams) { p.External.GameID = "" },
		"long game":             func(p *wager.ExternalParams) { p.External.GameID = long },
		"missing reference":     func(p *wager.ExternalParams) { p.Kind = wager.KindRefund },
		"long reference":        func(p *wager.ExternalParams) { p.Kind, p.External.ReferenceExternalID = wager.KindWin, long },
		"bet with reference":    func(p *wager.ExternalParams) { p.External.ReferenceExternalID = "x" },
		"negative amount (neg)": func(p *wager.ExternalParams) { p.Amount = p.Amount.Neg() },
	}
	for name, mutate := range mutations {
		p := f.params(t, wager.KindBet, "10.00", "")
		mutate(&p)
		_, err := wager.NewExternal(p)
		assert.Error(t, err, name)
	}
}

func TestNewOpening(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "0.00")
	p := wager.OpeningParams{ID: uuid.New(), WalletID: f.walletID, PlayerID: f.playerID, Amount: mny(t, "10.00", "BRL"), Now: now}
	tx, err := wager.NewOpening(p)
	require.NoError(t, err)
	assert.Equal(t, wager.KindOpening, tx.Kind())
	assert.Equal(t, wager.OriginInternal, tx.Origin())
	assert.Equal(t, wager.External{}, tx.External())
	assert.Equal(t, wager.StatusPending, tx.Status())

	p.Amount = mny(t, "0.00", "BRL")
	_, err = wager.NewOpening(p)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
	p.ID = uuid.Nil
	_, err = wager.NewOpening(p)
	require.ErrorIs(t, err, wager.ErrInvalidTransaction)
}

func TestTransitions(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	later := now.Add(time.Second)
	bal := mny(t, "75.00", "BRL")
	ref := uuid.New()

	processed := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, processed.Process(bal, ref, later))
	assert.Equal(t, wager.StatusProcessed, processed.Status())
	assert.Equal(t, bal, processed.ResultBalance())
	assert.Equal(t, ref, processed.ReferenceTxID())
	assert.Equal(t, later, processed.UpdatedAt())

	rejected := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, rejected.Reject(wager.CodeInsufficientFunds, bal, later))
	assert.Equal(t, wager.StatusRejected, rejected.Status())
	assert.Equal(t, wager.CodeInsufficientFunds, rejected.FailureCode())

	failed := f.tx(t, wager.KindBet, "25.00", "")
	require.NoError(t, failed.Fail(wager.CodeInternalFailure, later))
	assert.Equal(t, wager.StatusFailed, failed.Status())

	for _, terminal := range []*wager.Transaction{processed, rejected, failed} {
		assert.ErrorIs(t, terminal.Process(bal, uuid.Nil, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.Reject(wager.CodeInsufficientFunds, bal, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.Fail(wager.CodeInternalFailure, later), wager.ErrInvalidTransition)
		assert.ErrorIs(t, terminal.AwaitReference(later, later), wager.ErrInvalidTransition)
	}
}

func TestTransitionValidation(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	tx := f.tx(t, wager.KindBet, "25.00", "")
	assert.ErrorIs(t, tx.Process(money.Money{}, uuid.Nil, now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.Reject("", mny(t, "1.00", "BRL"), now), wager.ErrInvalidTransaction)
	assert.ErrorIs(t, tx.AwaitReference(now, now), wager.ErrInvalidTransition)
	assert.Equal(t, wager.StatusPending, tx.Status())
}

func TestAwaitReference(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	tx := f.tx(t, wager.KindRefund, "25.00", "bet-1")
	next := now.Add(time.Second)

	require.NoError(t, tx.AwaitReference(next, now))
	assert.Equal(t, wager.StatusPendingReference, tx.Status())
	assert.Equal(t, 1, tx.Attempts())
	assert.Equal(t, next, tx.NextAttemptAt())

	require.NoError(t, tx.AwaitReference(next.Add(time.Second), next))
	assert.Equal(t, 2, tx.Attempts())

	require.NoError(t, tx.Reject(wager.CodeReferenceNotFound, mny(t, "100.00", "BRL"), next))
	assert.Equal(t, wager.StatusRejected, tx.Status())
}

func TestRehydrate(t *testing.T) {
	t.Parallel()
	f := newFixture(t, "100.00")
	s := wager.Snapshot{
		ID: uuid.New(), Origin: wager.OriginExternal, Kind: wager.KindBet, Status: wager.StatusProcessed,
		WalletID: f.walletID, PlayerID: f.playerID, Amount: mny(t, "1.00", "BRL"),
		External: wager.External{ProviderID: "p"}, ResultBalance: mny(t, "99.00", "BRL"),
		Attempts: 3, CorrelationID: "c", CreatedAt: now, UpdatedAt: now,
	}
	tx, err := wager.Rehydrate(s)
	require.NoError(t, err)
	assert.Equal(t, 3, tx.Attempts())
	assert.Equal(t, wager.StatusProcessed, tx.Status())

	mutations := map[string]func(s *wager.Snapshot){
		"nil id":           func(s *wager.Snapshot) { s.ID = uuid.Nil },
		"bad kind":         func(s *wager.Snapshot) { s.Kind = "X" },
		"bad status":       func(s *wager.Snapshot) { s.Status = "X" },
		"bad amount":       func(s *wager.Snapshot) { s.Amount = money.Money{} },
		"internal non-op":  func(s *wager.Snapshot) { s.Origin = wager.OriginInternal },
		"external opening": func(s *wager.Snapshot) { s.Kind = wager.KindOpening },
	}
	for name, mutate := range mutations {
		c := s
		mutate(&c)
		_, err := wager.Rehydrate(c)
		assert.ErrorIs(t, err, wager.ErrInvalidTransaction, name)
	}
}

func TestFingerprint(t *testing.T) {
	t.Parallel()
	base := wager.Fingerprint{
		ProviderID: "provider-a", ExternalTransactionID: "t-1", PlayerID: "p", WalletID: "w",
		RoundID: "r", GameID: "g", Kind: wager.KindBet, Amount: "25.00", Currency: "BRL",
	}
	h := base.Hash()
	assert.Len(t, h, 64)
	assert.Equal(t, h, base.Hash(), "deterministic")
	// Canonical JSON: sorted keys, no whitespace, reference omitted when empty.
	canonical := `{"externalTransactionId":"t-1","gameId":"g","kind":"BET",` +
		`"money":{"amount":"25.00","currency":"BRL"},"playerId":"p","providerId":"provider-a","roundId":"r","walletId":"w"}`
	sum := sha256.Sum256([]byte(canonical))
	assert.Equal(t, hex.EncodeToString(sum[:]), h)

	changed := base
	changed.Amount = "25.01"
	assert.NotEqual(t, h, changed.Hash())
	withRef := base
	withRef.ReferenceExternalTransactionID = "t-0"
	assert.NotEqual(t, h, withRef.Hash())
}
