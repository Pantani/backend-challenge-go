// Package event defines the integration events emitted through the outbox.
//
// Every event has a concrete payload type and a constructor that fixes its
// type and schema version. Payloads only contain strings, integers and
// booleans: money travels as fixed-scale decimal strings and instants as UTC
// RFC 3339 strings, so the serialized snapshot is immutable and float-free.
package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// Event types.
const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
)

// SchemaVersion is the version of every payload defined in this package.
const SchemaVersion = 1

// Aggregate types used for routing.
const (
	AggregateWallet      = "Wallet"
	AggregateTransaction = "WagerTransaction"
)

// Money is the wire representation of money.Money.
type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// NewMoney converts a domain amount to its wire representation.
func NewMoney(m money.Money) Money {
	return Money{Amount: m.Amount(), Currency: string(m.Currency())}
}

// Envelope wraps a typed payload with routing and tracing metadata.
type Envelope[T any] struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	AggregateType string  `json:"aggregateType"`
	AggregateID   string  `json:"aggregateId"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId,omitempty"`
	OccurredAt    string  `json:"occurredAt"`
	Version       int     `json:"version"`
	Data          T       `json:"data"`
}

// Meta carries the tracing metadata shared by the events of one operation.
type Meta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

func newEnvelope[T any](m Meta, eventType, aggregateType string, aggregateID uuid.UUID, data T) Envelope[T] {
	var causation *string
	if m.CausationID != "" {
		c := m.CausationID
		causation = &c
	}
	return Envelope[T]{
		EventID: m.EventID.String(), EventType: eventType, AggregateType: aggregateType,
		AggregateID: aggregateID.String(), CorrelationID: m.CorrelationID, CausationID: causation,
		OccurredAt: FormatTime(m.OccurredAt), Version: SchemaVersion, Data: data,
	}
}

// FormatTime renders an instant as UTC RFC 3339 with millisecond precision.
func FormatTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// Record is the serialized, immutable snapshot stored in the outbox.
type Record struct {
	EventID       uuid.UUID
	EventType     string
	AggregateType string
	AggregateID   uuid.UUID
	PartitionKey  string
	OccurredAt    time.Time
	Payload       []byte
}

// ToRecord serializes an envelope. partitionKey groups events that must keep
// their relative order (the wallet id).
func ToRecord[T any](e Envelope[T], m Meta, aggregateID uuid.UUID, partitionKey string) Record {
	// Payloads only hold strings, ints and bools: Marshal cannot fail.
	payload, _ := json.Marshal(e)
	return Record{
		EventID: m.EventID, EventType: e.EventType, AggregateType: e.AggregateType,
		AggregateID: aggregateID, PartitionKey: partitionKey, OccurredAt: m.OccurredAt.UTC(), Payload: payload,
	}
}

// TransactionData is the common payload describing a wager transaction.
type TransactionData struct {
	TransactionID                  string `json:"transactionId"`
	Origin                         string `json:"origin"`
	Kind                           string `json:"kind"`
	Status                         string `json:"status"`
	WalletID                       string `json:"walletId"`
	PlayerID                       string `json:"playerId"`
	Money                          Money  `json:"money"`
	ProviderID                     string `json:"providerId,omitempty"`
	ExternalTransactionID          string `json:"externalTransactionId,omitempty"`
	RoundID                        string `json:"roundId,omitempty"`
	GameID                         string `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func newTransactionData(t *wager.Transaction) TransactionData {
	ext := t.External()
	return TransactionData{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()),
		Status: string(t.Status()), WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(),
		Money: NewMoney(t.Amount()), ProviderID: ext.ProviderID, ExternalTransactionID: ext.ExternalID,
		RoundID: ext.RoundID, GameID: ext.GameID, ReferenceExternalTransactionID: ext.ReferenceExternalID,
	}
}

// WagerTransactionProcessed is emitted when an operation succeeds (LOSS included).
type WagerTransactionProcessed struct {
	TransactionData
	ReferenceTransactionID string `json:"referenceTransactionId,omitempty"`
	Balance                Money  `json:"balance"`
}

// NewWagerTransactionProcessed builds the event for a processed transaction.
func NewWagerTransactionProcessed(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionProcessed{TransactionData: newTransactionData(t), Balance: NewMoney(t.ResultBalance())}
	if t.ReferenceTxID() != uuid.Nil {
		data.ReferenceTransactionID = t.ReferenceTxID().String()
	}
	env := newEnvelope(m, TypeWagerTransactionProcessed, AggregateTransaction, t.ID(), data)
	return ToRecord(env, m, t.ID(), t.WalletID().String())
}

// WagerTransactionRejected is emitted for definitive business rejections.
type WagerTransactionRejected struct {
	TransactionData
	FailureCode string `json:"failureCode"`
}

// NewWagerTransactionRejected builds the event for a rejected transaction.
func NewWagerTransactionRejected(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionRejected{TransactionData: newTransactionData(t), FailureCode: string(t.FailureCode())}
	env := newEnvelope(m, TypeWagerTransactionRejected, AggregateTransaction, t.ID(), data)
	return ToRecord(env, m, t.ID(), t.WalletID().String())
}

// WagerTransactionPendingReference is emitted when an operation starts waiting.
type WagerTransactionPendingReference struct {
	TransactionData
	NextAttemptAt string `json:"nextAttemptAt"`
}

// NewWagerTransactionPendingReference builds the event for a deferred operation.
func NewWagerTransactionPendingReference(m Meta, t *wager.Transaction) Record {
	data := WagerTransactionPendingReference{TransactionData: newTransactionData(t), NextAttemptAt: FormatTime(t.NextAttemptAt())}
	env := newEnvelope(m, TypeWagerTransactionPendingReference, AggregateTransaction, t.ID(), data)
	return ToRecord(env, m, t.ID(), t.WalletID().String())
}

// WalletBalanceChanged is emitted for every effective balance change.
type WalletBalanceChanged struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore Money  `json:"balanceBefore"`
	BalanceAfter  Money  `json:"balanceAfter"`
	WalletVersion int64  `json:"walletVersion"`
}

// NewWalletBalanceChanged builds the event from the ledger entry of a change.
func NewWalletBalanceChanged(m Meta, e wallet.LedgerEntry, walletVersion int64) Record {
	data := WalletBalanceChanged{
		WalletID: e.WalletID().String(), TransactionID: e.TransactionID().String(),
		Direction: string(e.Direction()), Money: NewMoney(e.Amount()),
		BalanceBefore: NewMoney(e.BalanceBefore()), BalanceAfter: NewMoney(e.BalanceAfter()),
		WalletVersion: walletVersion,
	}
	env := newEnvelope(m, TypeWalletBalanceChanged, AggregateWallet, e.WalletID(), data)
	return ToRecord(env, m, e.WalletID(), e.WalletID().String())
}
