package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-jose/go-jose/v4"
)

// maxJWKSBytes bounds the JWKS document read from the IdP.
const maxJWKSBytes = 1 << 20

// keySet verifies JWS signatures against the IdP's JWKS. Keys are fetched
// lazily and cached; a token whose signature matches no cached key triggers
// a refresh at most once per refresh interval, so a flood of forged tokens
// with random key ids cannot turn the API into a JWKS request amplifier.
type keySet struct {
	url      string
	client   *http.Client
	interval time.Duration
	algs     []jose.SignatureAlgorithm

	mu        sync.Mutex
	keys      []jose.JSONWebKey
	lastFetch time.Time
}

// VerifySignature implements oidc.KeySet: it returns the payload of jwt when
// a JWKS key with the token's key id verifies its signature.
func (k *keySet) VerifySignature(ctx context.Context, jwt string) ([]byte, error) {
	jws, err := jose.ParseSigned(jwt, k.algs)
	if err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}
	if len(jws.Signatures) != 1 {
		return nil, errors.New("token must carry exactly one signature")
	}
	kid := jws.Signatures[0].Header.KeyID
	if payload, ok := verify(jws, kid, k.cached()); ok {
		return payload, nil
	}
	keys, err := k.refresh(ctx)
	if err != nil {
		return nil, err
	}
	if payload, ok := verify(jws, kid, keys); ok {
		return payload, nil
	}
	return nil, errors.New("signature does not match any signing key")
}

// verify tries the keys whose id matches kid (any key when kid is empty).
func verify(jws *jose.JSONWebSignature, kid string, keys []jose.JSONWebKey) ([]byte, bool) {
	for i := range keys {
		if kid != "" && keys[i].KeyID != kid {
			continue
		}
		if payload, err := jws.Verify(&keys[i]); err == nil {
			return payload, true
		}
	}
	return nil, false
}

// cached returns the current keys without touching the network.
func (k *keySet) cached() []jose.JSONWebKey {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.keys
}

// refresh fetches the JWKS unless one was fetched within the interval, in
// which case the cached keys are returned so callers fail fast. Concurrent
// callers share a single fetch. Failed fetches also count against the
// interval so an unavailable IdP is not hammered.
func (k *keySet) refresh(ctx context.Context) ([]jose.JSONWebKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.lastFetch.IsZero() && time.Since(k.lastFetch) < k.interval {
		return k.keys, nil
	}
	k.lastFetch = time.Now()
	keys, err := k.fetch(ctx)
	if err != nil {
		return nil, err
	}
	k.keys = keys
	return keys, nil
}

// fetch downloads and parses the JWKS document.
func (k *keySet) fetch(ctx context.Context) ([]jose.JSONWebKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.url, nil)
	if err != nil {
		return nil, fmt.Errorf("building JWKS request: %w", err)
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching JWKS: unexpected status %d", resp.StatusCode)
	}
	var set jose.JSONWebKeySet
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxJWKSBytes)).Decode(&set); err != nil {
		return nil, fmt.Errorf("decoding JWKS: %w", err)
	}
	return set.Keys, nil
}
