//go:build integration || e2e

package testenv

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParallelReturnsWorkerErrorsInCallOrder(t *testing.T) {
	values, err := Parallel(3, func(i int) (int, error) {
		if i == 1 {
			return 0, errors.New("worker 1")
		}
		return i, nil
	})
	assert.Equal(t, []int{0, 0, 2}, values)
	require.ErrorContains(t, err, "worker 1")
}
