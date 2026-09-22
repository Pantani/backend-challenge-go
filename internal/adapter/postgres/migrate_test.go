package postgres

import (
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToPgx5URL(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgres://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("postgresql://h/db"))
	assert.Equal(t, "pgx5://h/db", toPgx5URL("pgx5://h/db"))
}

func TestIgnoreNoChange(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ignoreNoChange(nil))
	assert.NoError(t, ignoreNoChange(migrate.ErrNoChange))
	assert.NoError(t, ignoreNoChange(migrate.ErrShortLimit{Short: 2}))
	other := errors.New("dirty")
	assert.Same(t, other, ignoreNoChange(other))
}

func TestDownRejectsNonPositiveSteps(t *testing.T) {
	t.Parallel()
	m := &Migrator{}
	require.Error(t, m.Down(0))
	require.Error(t, m.Down(-1))
}
