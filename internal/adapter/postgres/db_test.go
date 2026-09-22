package postgres

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

func TestMapError(t *testing.T) {
	t.Parallel()
	assert.NoError(t, mapError(nil))
	cases := map[string]struct {
		err  error
		want error
	}{
		"unique":        {&pgconn.PgError{Code: "23505"}, app.ErrConflict},
		"serialization": {&pgconn.PgError{Code: "40001"}, app.ErrConflict},
		"deadlock":      {&pgconn.PgError{Code: "40P01"}, app.ErrConflict},
		"lock timeout":  {&pgconn.PgError{Code: "55P03"}, app.ErrConflict},
		"shutdown":      {&pgconn.PgError{Code: "57P01"}, app.ErrUnavailable},
		"connection":    {&pgconn.PgError{Code: "08006"}, app.ErrUnavailable},
		"network":       {&net.OpError{Op: "dial", Err: errors.New("refused")}, app.ErrUnavailable},
		"closed pool":   {errors.New("closed pool"), app.ErrUnavailable},
	}
	for name, tc := range cases {
		assert.ErrorIs(t, mapError(tc.err), tc.want, name)
	}
	check := &pgconn.PgError{Code: "23514"}
	assert.Same(t, check, mapError(check), "business constraint violations are not transient")
	plain := errors.New("plain")
	assert.Same(t, plain, mapError(plain))
}

func TestUniqueViolation(t *testing.T) {
	t.Parallel()
	assert.True(t, isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "c"}, "c"))
	assert.False(t, isUniqueViolation(&pgconn.PgError{Code: "23505", ConstraintName: "other"}, "c"))
	assert.False(t, isUniqueViolation(errors.New("x"), "c"))
}

func TestNewPoolValidation(t *testing.T) {
	t.Parallel()
	_, err := NewPool(context.Background(), Config{URL: "postgres://%%%"})
	require.Error(t, err)
	_, err = NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 0})
	require.Error(t, err, "pgxpool refuses MaxConns < 1")
	pool, err := NewPool(context.Background(), Config{URL: "postgres://u:p@localhost:1/db", MaxConns: 1, LockTimeout: time.Second})
	require.NoError(t, err, "the pool is lazy")
	pool.Close()
}

func TestToPgx5URL(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgres://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgresql://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("pgx5://h/db"))
}
