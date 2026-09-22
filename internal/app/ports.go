package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// WalletRepository persists wallets inside a unit of work.
type WalletRepository interface {
	// Create inserts a wallet; ErrWalletExists on (player, currency) conflict.
	Create(ctx context.Context, w *wallet.Wallet) error
	// GetForUpdate loads and row-locks a wallet; ErrWalletNotFound if absent.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Save updates balance and version only if the stored version still
	// equals expectedVersion; ErrConflict otherwise.
	Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

// TransactionRepository persists wager transactions inside a unit of work.
type TransactionRepository interface {
	// Create inserts a transaction; ErrConflict on unique violations.
	Create(ctx context.Context, t *wager.Transaction) error
	// Save persists a transition of a non-terminal stored transaction.
	Save(ctx context.Context, t *wager.Transaction) error
	// FindExisting returns transactions of the provider matching the
	// idempotency key or the external id.
	FindExisting(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*wager.Transaction, error)
	// GetByExternal returns ErrTransactionNotFound when absent.
	GetByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// HasProcessedReversal reports whether a REFUND or ROLLBACK of the
	// referenced transaction was already processed.
	HasProcessedReversal(ctx context.Context, referenceTxID uuid.UUID) (bool, error)
	// LockDuePending row-locks a PENDING_REFERENCE transaction that is due,
	// skipping rows locked by other instances; ErrNotDue otherwise.
	LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error)
	// WakeDependents makes pending operations referencing the given external
	// transaction due immediately.
	WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) error
}

// LedgerRepository appends immutable ledger entries.
type LedgerRepository interface {
	Append(ctx context.Context, e wallet.LedgerEntry) error
}

// OutboxRepository stores integration events in the same SQL transaction.
type OutboxRepository interface {
	Append(ctx context.Context, records ...event.Record) error
}

// InboxEntry is the durable record of a consumed message.
type InboxEntry struct {
	PayloadHash string
	Completed   bool
}

// InboxRepository deduplicates consumed messages.
type InboxRepository interface {
	// Register inserts (consumer, messageID) or returns the existing entry
	// with created=false.
	Register(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (entry InboxEntry, created bool, err error)
	// Complete marks the message as durably handled.
	Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error
}

// Repositories groups the repositories bound to one SQL transaction.
type Repositories interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// UnitOfWork runs fn inside a single SQL transaction, committing when fn
// returns nil and rolling back otherwise.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
}

// LedgerRow is a ledger entry with its stable ordering key.
type LedgerRow struct {
	Seq   int64
	Entry wallet.LedgerEntry
}

// ReconciliationSnapshot is read in one consistent view of the data.
type ReconciliationSnapshot struct {
	Stored  money.Money
	Credits int64
	Debits  int64
	Entries int64
}

// DueTransaction identifies a pending operation ready for a new attempt.
type DueTransaction struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

// Queries is the read side, executed outside units of work.
type Queries interface {
	GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	ListLedger(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetTransactionByExternal(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	Reconcile(ctx context.Context, walletID uuid.UUID) (ReconciliationSnapshot, error)
	ListDuePending(ctx context.Context, now time.Time, limit int) ([]DueTransaction, error)
}

// Clock returns the current instant.
type Clock interface{ Now() time.Time }

// SystemClock is the wall clock in UTC.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator creates identifiers.
type IDGenerator interface{ New() uuid.UUID }

// UUIDv7 generates time-ordered UUIDs.
type UUIDv7 struct{}

// New implements IDGenerator.
func (UUIDv7) New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// Metrics records business metrics.
type Metrics interface {
	TransactionResult(source string, status wager.Status, replay bool)
	ConcurrencyConflict(operation string)
	ProcessingDuration(source string, d time.Duration)
	ReconciliationDivergence()
	PendingResolution(outcome wager.Status)
}

// OutboxMessage is a claimed, not yet published outbox record.
type OutboxMessage struct {
	Seq           int64
	EventID       uuid.UUID
	EventType     string
	AggregateType string
	AggregateID   uuid.UUID
	PartitionKey  string
	Payload       []byte
	OccurredAt    time.Time
	Attempts      int
}

// OutboxStore is used by the relay, outside business transactions.
type OutboxStore interface {
	// Claim leases up to limit due records to owner until now+lease. Records
	// whose lease expired (a crashed publisher) are claimable again.
	Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]OutboxMessage, error)
	// MarkPublished confirms a publication; false when the lease was lost.
	MarkPublished(ctx context.Context, eventID uuid.UUID, owner string, now time.Time) (bool, error)
	// MarkFailed releases the lease and schedules the next attempt.
	MarkFailed(ctx context.Context, eventID uuid.UUID, owner string, next time.Time, cause string) error
	// MarkDead dead-letters a record that exhausted its attempts, so it stops
	// blocking the later records of its partition (kept for audit/replay).
	MarkDead(ctx context.Context, eventID uuid.UUID, owner string, now time.Time, cause string) error
	// OldestPending returns the occurrence of the oldest unpublished record.
	OldestPending(ctx context.Context) (time.Time, bool, error)
}
