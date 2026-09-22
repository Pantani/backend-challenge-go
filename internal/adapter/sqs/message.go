package sqs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/Pantani/backend-challenge-go/internal/app"
)

// MessageType is the only accepted envelope type.
const MessageType = "WagerTransactionRequested"

// ErrInvalidMessage reports an undecodable or invalid message; it is sent to
// the DLQ without retries.
var ErrInvalidMessage = errors.New("invalid message")

type moneyData struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type messageData struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	IdempotencyKey                 string    `json:"idempotencyKey"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          moneyData `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
}

// Envelope is the wager-transactions message contract.
type Envelope struct {
	MessageID  string      `json:"messageId"`
	Type       string      `json:"type"`
	OccurredAt string      `json:"occurredAt"`
	Data       messageData `json:"data"`
}

// decodeEnvelope reads exactly one JSON object with no unknown fields and
// nothing after it.
func decodeEnvelope(body string) (Envelope, error) {
	var env Envelope
	dec := json.NewDecoder(bytes.NewReader([]byte(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Envelope{}, fmt.Errorf("%w: body must contain a single JSON object", ErrInvalidMessage)
	}
	return env, nil
}

// DecodeMessage parses and validates a message body with the same rules as
// HTTP. The envelope messageId is the durable message identity; the inbox
// hash covers the type, the idempotency key and the business payload hash.
func DecodeMessage(consumer, body string) (app.InboundMessage, error) {
	env, err := decodeEnvelope(body)
	if err != nil {
		return app.InboundMessage{}, err
	}
	if env.MessageID == "" || env.Type != MessageType {
		return app.InboundMessage{}, fmt.Errorf("%w: messageId is required and type must be %s", ErrInvalidMessage, MessageType)
	}
	d := env.Data
	cmd, err := app.NewSubmitCommand(app.SubmitInput{
		ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey,
		PlayerID: d.PlayerID, WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind,
		Amount: d.Money.Amount, Currency: d.Money.Currency, ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
		CorrelationID: env.MessageID, CausationID: env.MessageID,
	})
	if err != nil {
		return app.InboundMessage{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	sum := sha256.Sum256([]byte(env.Type + "\n" + cmd.IdempotencyKey + "\n" + cmd.PayloadHash))
	return app.InboundMessage{Consumer: consumer, MessageID: env.MessageID, Hash: hex.EncodeToString(sum[:]), Command: cmd}, nil
}
