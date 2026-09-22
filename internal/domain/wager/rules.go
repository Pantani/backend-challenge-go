package wager

import (
	"slices"

	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Action is the outcome chosen by Decide.
type Action int

// Decision outcomes.
const (
	ActionProcess Action = iota + 1
	ActionReject
	ActionAwait
)

// Decision tells the application how to conclude an operation.
type Decision struct {
	Action    Action
	Code      FailureCode
	Moves     bool
	Direction wallet.Direction
}

// DecisionInput gathers everything the rules need. Reference is nil when the
// referenced operation has not arrived; ReferenceReversed reports whether it
// already has a successful REFUND or ROLLBACK.
type DecisionInput struct {
	Wallet            *wallet.Wallet
	Transaction       *Transaction
	Reference         *Transaction
	ReferenceReversed bool
}

func reject(code FailureCode) Decision { return Decision{Action: ActionReject, Code: code} }

// Decide applies the business rules of the five external kinds. It is pure:
// it never mutates the wallet or the transaction.
func Decide(in DecisionInput) Decision {
	if code := checkOwnership(in.Wallet, in.Transaction); code != "" {
		return reject(code)
	}
	if d, done := decideReference(in); done {
		return d
	}
	return decideMovement(in.Wallet, in.Transaction, in.Reference)
}

func checkOwnership(w *wallet.Wallet, t *Transaction) FailureCode {
	if w.PlayerID() != t.PlayerID() || w.ID() != t.WalletID() {
		return CodePlayerWalletMismatch
	}
	if w.Currency() != t.Amount().Currency() {
		return CodeCurrencyMismatch
	}
	return ""
}

func decideReference(in DecisionInput) (Decision, bool) {
	t, ref := in.Transaction, in.Reference
	if t.External().ReferenceExternalID == "" {
		return Decision{}, false
	}
	if ref == nil || !ref.Status().Terminal() {
		return Decision{Action: ActionAwait}, true
	}
	if code := checkReference(t, ref, in.ReferenceReversed); code != "" {
		return reject(code), true
	}
	return Decision{}, false
}

func checkReference(t, ref *Transaction, reversed bool) FailureCode {
	if ref.Status() != StatusProcessed {
		return CodeReferenceNotProcessed
	}
	if !sameContext(t, ref) {
		return CodeReferenceMismatch
	}
	return checkTarget(t, ref, reversed)
}

func checkTarget(t, ref *Transaction, reversed bool) FailureCode {
	if !reversible(t.Kind(), ref.Kind()) {
		return CodeReferenceKindInvalid
	}
	if t.Kind().IsReversal() && !t.Amount().Equal(ref.Amount()) {
		return CodeReversalAmountMismatch
	}
	if t.Kind().IsReversal() && reversed {
		return CodeAlreadyReversed
	}
	return ""
}

// sameContext requires provider, player, wallet, currency and round to match.
func sameContext(t, ref *Transaction) bool {
	a, b := t.External(), ref.External()
	sameOwner := t.PlayerID() == ref.PlayerID() && t.WalletID() == ref.WalletID()
	return sameOwner && a.ProviderID == b.ProviderID && a.RoundID == b.RoundID &&
		t.Amount().Currency() == ref.Amount().Currency()
}

// referenceTargets lists which kinds each referencing kind may point to.
var referenceTargets = map[Kind][]Kind{
	KindWin:      {KindBet},
	KindRefund:   {KindBet},
	KindRollback: {KindBet, KindWin, KindRefund},
}

func reversible(kind, target Kind) bool {
	return slices.Contains(referenceTargets[kind], target)
}

// directionOf returns the wallet movement of a kind. ROLLBACK moves opposite
// to the operation it undoes; LOSS does not move money.
func directionOf(kind Kind, ref *Transaction) (wallet.Direction, bool) {
	switch kind {
	case KindBet:
		return wallet.Debit, true
	case KindWin, KindRefund, KindOpening:
		return wallet.Credit, true
	case KindRollback:
		d, _ := directionOf(ref.Kind(), nil)
		return d.Opposite(), true
	}
	return "", false
}

func decideMovement(w *wallet.Wallet, t *Transaction, ref *Transaction) Decision {
	dir, moves := directionOf(t.Kind(), ref)
	if !moves {
		return Decision{Action: ActionProcess}
	}
	if dir == wallet.Debit && !w.CanDebit(t.Amount()) {
		return reject(insufficientFundsCode(t.Kind()))
	}
	if _, err := w.Balance().Add(t.Amount()); dir == wallet.Credit && err != nil {
		return reject(CodeBalanceLimitExceeded)
	}
	return Decision{Action: ActionProcess, Moves: true, Direction: dir}
}

// insufficientFundsCode keeps reversal shortfalls distinguishable from bets.
func insufficientFundsCode(kind Kind) FailureCode {
	if kind.IsReversal() {
		return CodeReversalInsufficientFunds
	}
	return CodeInsufficientFunds
}
