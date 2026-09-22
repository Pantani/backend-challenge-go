package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

var errBoom = errors.New("boom")

type failingRow struct{}

func (failingRow) Scan(...any) error { return errBoom }

// failingDB fails every statement, as a dropped connection would.
type failingDB struct{}

func (failingDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errBoom
}
func (failingDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, errBoom }
func (failingDB) QueryRow(context.Context, string, ...any) pgx.Row        { return failingRow{} }

func TestScanFailures(t *testing.T) {
	t.Parallel()
	_, err := scanLedgerRow(failingRow{})
	require.ErrorIs(t, err, errBoom)
	_, err = scanTransaction(failingRow{})
	require.ErrorIs(t, err, errBoom)
	_, err = queryTransactions(context.Background(), failingDB{}, "SELECT 1")
	require.ErrorIs(t, err, errBoom)
}

func TestIgnoreTxClosed(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ignoreTxClosed(nil))
	assert.NoError(t, ignoreTxClosed(pgx.ErrTxClosed))
	assert.ErrorIs(t, ignoreTxClosed(&pgconn.PgError{Code: "57P01"}), app.ErrUnavailable)
}
