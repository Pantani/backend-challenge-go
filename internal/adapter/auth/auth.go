// Package auth validates OAuth 2.0 access tokens issued by an external OIDC
// provider (Keycloak) and maps them to principals.
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Realm roles granted by the IdP.
const (
	// RoleProvider allows submitting and reading the provider's own operations.
	RoleProvider = "wager-provider"
	// RoleWalletAdmin is held by the internal service; it allows wallet
	// operations and reading every transaction.
	RoleWalletAdmin = "wallet-admin"
)

// ErrUnauthenticated reports a missing, malformed, invalid or expired token.
var ErrUnauthenticated = errors.New("unauthenticated")

// Principal is the authenticated caller.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      []string
}

// HasRole reports whether the principal holds a realm role.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// IsInternal reports whether the caller is the internal wallet service.
func (p Principal) IsInternal() bool { return p.HasRole(RoleWalletAdmin) }

// IsProvider reports whether the caller is a game provider bound to a
// providerId. The providerId comes only from the token, never from input.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// Config configures token validation.
type Config struct {
	// Issuer is the expected "iss" claim.
	Issuer string
	// JWKSURL is where signing keys are fetched (may differ from the issuer
	// host inside Docker networks).
	JWKSURL string
	// Audience is the expected "aud" entry for this API.
	Audience string
}

// Verifier validates access tokens.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewVerifier builds a verifier backed by the remote JWKS. Keys are fetched
// lazily and cached, and refreshed when an unknown key id appears.
func NewVerifier(ctx context.Context, cfg Config) *Verifier {
	keys := oidc.NewRemoteKeySet(ctx, cfg.JWKSURL)
	return &Verifier{verifier: oidc.NewVerifier(cfg.Issuer, keys, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})}
}

type claims struct {
	AuthorizedParty string `json:"azp"`
	ProviderID      string `json:"provider_id"`
	RealmAccess     struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify checks signature, issuer, audience and expiry, then extracts the
// principal.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: %w", ErrUnauthenticated, err)
	}
	return Principal{Subject: tok.Subject, ClientID: c.AuthorizedParty, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, nil
}
