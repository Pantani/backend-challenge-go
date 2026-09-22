package cli

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeVersioner struct{ err error }

func (f fakeVersioner) Version() (uint, bool, error) { return 3, true, f.err }

func TestPrintVersion(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	require.NoError(t, printVersion(fakeVersioner{}, &out))
	assert.Equal(t, "version=3 dirty=true\n", out.String())
	boom := errors.New("boom")
	require.ErrorIs(t, printVersion(fakeVersioner{err: boom}, &out), boom)
}
