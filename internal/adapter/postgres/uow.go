package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// UnitOfWork opens one READ COMMITTED transaction per call. Wallet rows are
// locked explicitly (SELECT ... FOR UPDATE) and balance updates are guarded
// by the version column, so READ COMMITTED is enough and never aborts
// independent wallets.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork builds a UnitOfWork over the pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Do implements app.UnitOfWork.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, r app.Repositories) error) error {
	tx, err := u.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapError(err)
	}
	if err := fn(ctx, repositories{tx: tx}); err != nil {
		return errors.Join(err, rollback(ctx, tx))
	}
	return mapError(tx.Commit(ctx))
}

// rollbackTimeout bounds the rollback of a cancelled request.
const rollbackTimeout = 5 * time.Second

// rollback detaches from the request cancellation so a cancelled request
// still releases its locks promptly; failures only matter when the
// connection is unusable (pgx then discards it).
func rollback(ctx context.Context, tx pgx.Tx) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rollbackTimeout)
	defer cancel()
	return ignoreTxClosed(tx.Rollback(ctx))
}

// ignoreTxClosed treats an already finished transaction as rolled back.
func ignoreTxClosed(err error) error {
	if errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return mapError(err)
}

type repositories struct{ tx pgx.Tx }

func (r repositories) Wallets() app.WalletRepository           { return walletRepo{db: r.tx} }
func (r repositories) Transactions() app.TransactionRepository { return transactionRepo{db: r.tx} }
func (r repositories) Ledger() app.LedgerRepository            { return ledgerRepo{db: r.tx} }
func (r repositories) Outbox() app.OutboxRepository            { return outboxRepo{db: r.tx} }
func (r repositories) Inbox() app.InboxRepository              { return inboxRepo{db: r.tx} }
