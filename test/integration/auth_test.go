//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
)

func verifier() *auth.Verifier {
	return auth.NewVerifier(context.Background(), auth.Config{Issuer: env.Issuer(), JWKSURL: env.JWKSURL(), Audience: "wallet-api"})
}

func token(t *testing.T, client string) string {
	t.Helper()
	raw, err := env.Token(context.Background(), client)
	require.NoError(t, err)
	return raw
}

func TestKeycloakIssuesProviderAndInternalIdentities(t *testing.T) {
	t.Parallel()
	v := verifier()
	ctx := context.Background()

	a, err := v.Verify(ctx, token(t, "provider-a"))
	require.NoError(t, err)
	assert.Equal(t, "provider-a", a.ProviderID)
	assert.True(t, a.IsProvider())
	assert.False(t, a.IsInternal())

	b, err := v.Verify(ctx, token(t, "provider-b"))
	require.NoError(t, err)
	assert.Equal(t, "provider-b", b.ProviderID)

	internal, err := v.Verify(ctx, token(t, "wallet-service"))
	require.NoError(t, err)
	assert.True(t, internal.IsInternal())
	assert.Empty(t, internal.ProviderID)

	none, err := v.Verify(ctx, token(t, "no-role-client"))
	require.NoError(t, err)
	assert.False(t, none.IsProvider())
	assert.False(t, none.IsInternal())
}

func TestKeycloakRejectsExpiredAndTamperedTokens(t *testing.T) {
	t.Parallel()
	v := verifier()
	ctx := context.Background()
	shortLived := token(t, "provider-a-short-lived")
	_, err := v.Verify(ctx, shortLived)
	require.NoError(t, err, "valid right after issuance")

	time.Sleep(4 * time.Second)
	_, err = v.Verify(ctx, shortLived)
	require.ErrorIs(t, err, auth.ErrUnauthenticated, "expired")

	raw := token(t, "provider-a")
	tampered := raw[:len(raw)-4] + "AAAA"
	_, err = v.Verify(ctx, tampered)
	require.ErrorIs(t, err, auth.ErrUnauthenticated, "bad signature")

	wrongAudience := auth.NewVerifier(ctx, auth.Config{Issuer: env.Issuer(), JWKSURL: env.JWKSURL(), Audience: "another-api"})
	_, err = wrongAudience.Verify(ctx, raw)
	require.ErrorIs(t, err, auth.ErrUnauthenticated, "audience")
}
