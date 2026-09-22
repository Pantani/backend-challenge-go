package bootstrap_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/Pantani/backend-challenge-go/internal/bootstrap"
	"github.com/Pantani/backend-challenge-go/internal/config"
)

// TestGraphIsComplete checks the dependency graph without starting anything.
func TestGraphIsComplete(t *testing.T) {
	t.Parallel()
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	require.NoError(t, fx.ValidateApp(bootstrap.Options(cfg)))
}
