package auth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Pantani/backend-challenge-go/internal/adapter/auth"
)

const (
	issuer   = "http://idp.test/realms/wallet"
	audience = "wallet-api"
)

// idp is a local OIDC key server that signs tokens like Keycloak does.
type idp struct {
	key    *rsa.PrivateKey
	server *httptest.Server
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks := jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(srv.Close)
	return &idp{key: key, server: srv}
}

func (i *idp) sign(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	require.NoError(t, err)
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)
	return raw
}

func (i *idp) verifier() *auth.Verifier {
	return auth.NewVerifier(context.Background(), auth.Config{Issuer: issuer, JWKSURL: i.server.URL, Audience: audience})
}

func claims(overrides map[string]any) map[string]any {
	c := map[string]any{
		"iss": issuer, "aud": []string{audience, "account"}, "sub": "service-account-provider-a",
		"exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "azp": "provider-a",
		"provider_id":  "provider-a",
		"realm_access": map[string]any{"roles": []string{auth.RoleProvider, "offline_access"}},
	}
	for k, v := range overrides {
		c[k] = v
	}
	return c
}

func TestVerifyValidProviderToken(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	p, err := i.verifier().Verify(context.Background(), i.sign(t, i.key, claims(nil)))
	require.NoError(t, err)
	assert.Equal(t, "provider-a", p.ProviderID)
	assert.Equal(t, "provider-a", p.ClientID)
	assert.Equal(t, "service-account-provider-a", p.Subject)
	assert.True(t, p.IsProvider())
	assert.False(t, p.IsInternal())
}

func TestVerifyInternalToken(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	raw := i.sign(t, i.key, claims(map[string]any{"provider_id": nil, "realm_access": map[string]any{"roles": []string{auth.RoleWalletAdmin}}}))
	p, err := i.verifier().Verify(context.Background(), raw)
	require.NoError(t, err)
	assert.True(t, p.IsInternal())
	assert.False(t, p.IsProvider())
}

func TestVerifyRejectsInvalidTokens(t *testing.T) {
	t.Parallel()
	i := newIDP(t)
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	cases := map[string]string{
		"malformed":      "not-a-jwt",
		"expired":        i.sign(t, i.key, claims(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})),
		"wrong issuer":   i.sign(t, i.key, claims(map[string]any{"iss": "http://evil"})),
		"wrong audience": i.sign(t, i.key, claims(map[string]any{"aud": "other-api"})),
		"bad signature":  i.sign(t, other, claims(nil)),
		"bad claims":     i.sign(t, i.key, claims(map[string]any{"realm_access": "admin"})),
	}
	for name, raw := range cases {
		_, err := i.verifier().Verify(context.Background(), raw)
		assert.ErrorIs(t, err, auth.ErrUnauthenticated, name)
	}
}

func TestPrincipalRoles(t *testing.T) {
	t.Parallel()
	p := auth.Principal{Roles: []string{auth.RoleProvider}}
	assert.True(t, p.HasRole(auth.RoleProvider))
	assert.False(t, p.IsProvider(), "a provider role without providerId is not a provider")
}
