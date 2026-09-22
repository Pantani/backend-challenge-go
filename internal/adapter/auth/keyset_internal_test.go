package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go-oidc parses the compact token before asking the key set to verify it,
// so these keySet branches are unreachable through Verifier.Verify.
func TestKeySetRejectsUnparsableAndMultiSignedTokens(t *testing.T) {
	t.Parallel()
	k := &keySet{algs: []jose.SignatureAlgorithm{jose.RS256}}
	_, err := k.VerifySignature(context.Background(), "not-a-jws")
	assert.ErrorContains(t, err, "parsing token")

	a, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	b, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signer, err := jose.NewMultiSigner([]jose.SigningKey{{Algorithm: jose.RS256, Key: a}, {Algorithm: jose.RS256, Key: b}}, nil)
	require.NoError(t, err)
	jws, err := signer.Sign([]byte(`{"sub":"x"}`))
	require.NoError(t, err)
	_, err = k.VerifySignature(context.Background(), jws.FullSerialize())
	assert.ErrorContains(t, err, "exactly one signature")
}
