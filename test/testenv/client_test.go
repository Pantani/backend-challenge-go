//go:build integration || e2e

package testenv

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelReturnsAllWorkerErrors(t *testing.T) {
	first, second := errors.New("worker 0"), errors.New("worker 2")
	values, err := Parallel(3, func(i int) (int, error) {
		switch i {
		case 0:
			return 0, first
		case 2:
			return 0, second
		default:
			return i, nil
		}
	})
	assert.Equal(t, []int{0, 1, 0}, values)
	require.ErrorIs(t, err, first)
	require.ErrorIs(t, err, second)
}
