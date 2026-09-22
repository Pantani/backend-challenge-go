// Package postgres implements the application ports with pgx and explicit SQL.
//
// Money is stored as BIGINT minor units plus a CHAR(3) currency. Every write
// of a use case runs inside the single pgx.Tx opened by UnitOfWork.Do, which
// the repositories of that call share; reads that need no locks use the pool.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// Config configures the connection pool.
type Config struct {
	URL              string
	MaxConns         int32
	LockTimeout      time.Duration
	StatementTimeout time.Duration
}

// NewPool builds a lazy pool (connections are opened on demand; callers
// Ping it to validate the dependency). lock_timeout and statement_timeout bound
// how long a request can wait on a wallet lock or a slow statement.
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.ConnConfig.RuntimeParams["lock_timeout"] = fmt.Sprint(cfg.LockTimeout.Milliseconds())
	pc.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(cfg.StatementTimeout.Milliseconds())
	pc.ConnConfig.RuntimeParams["application_name"] = "wallet-service"
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return pool, nil
}

// Ping validates connectivity, classifying failures as ErrUnavailable.
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return mapError(pool.Ping(ctx))
}

// dbtx is satisfied by both *pgxpool.Pool and pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// sqlStateErrors maps SQLSTATE codes to application errors: lost races are
// retryable conflicts; server shutdown and overload are transient outages.
var sqlStateErrors = map[string]error{
	"23505": app.ErrConflict,    // unique_violation
	"40001": app.ErrConflict,    // serialization_failure
	"40P01": app.ErrConflict,    // deadlock_detected
	"55P03": app.ErrConflict,    // lock_not_available (lock_timeout)
	"57014": app.ErrUnavailable, // query_canceled (statement_timeout)
	"57P01": app.ErrUnavailable, // admin_shutdown
	"57P02": app.ErrUnavailable, // crash_shutdown
	"57P03": app.ErrUnavailable, // cannot_connect_now
	"53300": app.ErrUnavailable, // too_many_connections
}

// mapError classifies driver errors into application errors.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return mapPgError(pgErr)
	}
	if isConnectionError(err) {
		return fmt.Errorf("%w: %w", app.ErrUnavailable, err)
	}
	return err
}

func mapPgError(e *pgconn.PgError) error {
	if mapped, ok := sqlStateErrors[e.Code]; ok {
		return fmt.Errorf("%w: %w", mapped, e)
	}
	if strings.HasPrefix(e.Code, "08") { // connection_exception class
		return fmt.Errorf("%w: %w", app.ErrUnavailable, e)
	}
	return e
}

func isConnectionError(err error) bool {
	var connErr *pgconn.ConnectError
	var netErr net.Error
	return errors.As(err, &connErr) || errors.As(err, &netErr) || pgconn.Timeout(err) ||
		strings.Contains(err.Error(), "closed pool") || strings.Contains(err.Error(), "conn closed")
}

// isUniqueViolation reports a unique violation on the named constraint.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}

// nullString maps "" to SQL NULL.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
