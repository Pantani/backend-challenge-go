package wager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Fingerprint holds the normalized business fields of an external operation.
// The idempotency key and transport metadata (headers, SQS envelope, message
// id, timestamps, correlation) are deliberately excluded, so HTTP and SQS
// produce the same hash for the same operation.
type Fingerprint struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// Hash returns the hex SHA-256 of the canonical JSON of the fingerprint:
// object keys sorted lexicographically, no insignificant whitespace, money as
// the fixed two-decimal string and the absent reference omitted.
func (f Fingerprint) Hash() string {
	doc := map[string]any{
		"providerId":            f.ProviderID,
		"externalTransactionId": f.ExternalTransactionID,
		"playerId":              f.PlayerID,
		"walletId":              f.WalletID,
		"roundId":               f.RoundID,
		"gameId":                f.GameID,
		"kind":                  string(f.Kind),
		"money":                 map[string]any{"amount": f.Amount, "currency": f.Currency},
	}
	if f.ReferenceExternalTransactionID != "" {
		doc["referenceExternalTransactionId"] = f.ReferenceExternalTransactionID
	}
	// encoding/json sorts map keys, which yields the canonical form. Marshal
	// cannot fail for maps of strings.
	canonical, _ := json.Marshal(doc)
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
