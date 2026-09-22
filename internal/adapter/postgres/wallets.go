package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

type walletRepo struct{ db dbtx }

func (r walletRepo) Create(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.db.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if isUniqueViolation(err, "wallets_player_currency_key") {
		return app.ErrWalletExists
	}
	return mapError(err)
}

func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return getWallet(ctx, r.db, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
}

// Save is a conditional update on the version read under the row lock: even
// if a caller forgot the lock, a stale writer gets ErrConflict instead of
// overwriting a committed balance (no lost updates).
func (r walletRepo) Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.db.Exec(ctx,
		`UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConflict
	}
	return nil
}

func getWallet(ctx context.Context, db dbtx, query string, id uuid.UUID) (*wallet.Wallet, error) {
	var (
		s        wallet.Snapshot
		currency string
		minor    int64
	)
	err := db.QueryRow(ctx, query, id).Scan(&s.ID, &s.PlayerID, &currency, &minor, &s.Version, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrWalletNotFound
	}
	if err != nil {
		return nil, mapError(err)
	}
	if s.Balance, err = money.FromMinor(minor, money.Currency(currency)); err != nil {
		return nil, err
	}
	return wallet.Rehydrate(s)
}

type ledgerRepo struct{ db dbtx }

func (r ledgerRepo) Append(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.db.Exec(ctx, `INSERT INTO ledger_entries
		(id, wallet_id, transaction_id, direction, amount_minor, currency, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(),
		string(e.Amount().Currency()), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return mapError(err)
}

const ledgerColumns = `seq, id, wallet_id, transaction_id, direction, amount_minor, currency,
	balance_before_minor, balance_after_minor, created_at`

func scanLedgerRow(row pgx.Row) (app.LedgerRow, error) {
	var (
		seq                   int64
		p                     wallet.LedgerEntryParams
		direction, currency   string
		amount, before, after int64
		createdAt             time.Time
	)
	if err := row.Scan(&seq, &p.ID, &p.WalletID, &p.TransactionID, &direction, &amount, &currency, &before, &after, &createdAt); err != nil {
		return app.LedgerRow{}, mapError(err)
	}
	c := money.Currency(currency)
	p.Direction, p.CreatedAt = wallet.Direction(direction), createdAt
	p.Amount, _ = money.FromMinor(amount, c)
	p.BalanceBefore, _ = money.FromMinor(before, c)
	p.BalanceAfter, _ = money.FromMinor(after, c)
	entry, err := wallet.NewLedgerEntry(p)
	return app.LedgerRow{Seq: seq, Entry: entry}, err
}
