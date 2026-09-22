package app

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// SubmitInput is the raw operation shared by HTTP and SQS. Every field is a
// string so that both transports go through exactly the same validation and
// normalization before the payload hash is computed.
type SubmitInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
	CorrelationID                  string
	CausationID                    string
}

// SubmitCommand is a validated operation.
type SubmitCommand struct {
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PlayerID            uuid.UUID
	WalletID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                wager.Kind
	Amount              money.Money
	ReferenceExternalID string
	PayloadHash         string
	CorrelationID       string
	CausationID         string
}

// NewSubmitCommand validates and normalizes a raw operation. UUIDs are
// normalized to their canonical lowercase form before hashing; amounts are
// only accepted in the canonical two-decimal form, so no other normalization
// is needed.
func NewSubmitCommand(in SubmitInput) (SubmitCommand, error) {
	ids, err := parseUUIDs(in.PlayerID, in.WalletID)
	if err != nil {
		return SubmitCommand{}, err
	}
	kind, err := wager.ParseExternalKind(in.Kind)
	if err != nil {
		return SubmitCommand{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	amount, err := money.Parse(in.Amount, in.Currency)
	if err != nil {
		return SubmitCommand{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	cmd := SubmitCommand{
		ProviderID: in.ProviderID, ExternalID: in.ExternalTransactionID, IdempotencyKey: in.IdempotencyKey,
		PlayerID: ids[0], WalletID: ids[1], RoundID: in.RoundID, GameID: in.GameID, Kind: kind,
		Amount: amount, ReferenceExternalID: in.ReferenceExternalTransactionID,
		CorrelationID: in.CorrelationID, CausationID: in.CausationID,
	}
	cmd.PayloadHash = cmd.fingerprint().Hash()
	if err := cmd.validate(); err != nil {
		return SubmitCommand{}, err
	}
	return cmd, nil
}

func parseUUIDs(values ...string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(values))
	for i, v := range values {
		id, err := uuid.Parse(v)
		if err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("%w: invalid uuid %q", ErrValidation, v)
		}
		out[i] = id
	}
	return out, nil
}

func (c SubmitCommand) fingerprint() wager.Fingerprint {
	return wager.Fingerprint{
		ProviderID: c.ProviderID, ExternalTransactionID: c.ExternalID,
		PlayerID: c.PlayerID.String(), WalletID: c.WalletID.String(), RoundID: c.RoundID, GameID: c.GameID,
		Kind: c.Kind, Amount: c.Amount.Amount(), Currency: string(c.Amount.Currency()),
		ReferenceExternalTransactionID: c.ReferenceExternalID,
	}
}

// validate runs the domain constructor on throwaway identifiers so every
// transport rejects the same invalid inputs before touching the database.
func (c SubmitCommand) validate() error {
	_, err := wager.NewExternal(c.params(uuid.Max, SystemClock{}.Now()))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}
	return nil
}

func (c SubmitCommand) params(id uuid.UUID, now time.Time) wager.ExternalParams {
	return wager.ExternalParams{
		ID: id, WalletID: c.WalletID, PlayerID: c.PlayerID, Kind: c.Kind, Amount: c.Amount,
		CorrelationID: c.CorrelationID, Now: now,
		External: wager.External{
			ProviderID: c.ProviderID, ExternalID: c.ExternalID, IdempotencyKey: c.IdempotencyKey,
			PayloadHash: c.PayloadHash, RoundID: c.RoundID, GameID: c.GameID,
			ReferenceExternalID: c.ReferenceExternalID,
		},
	}
}

// OpenWalletInput is the raw wallet opening request.
type OpenWalletInput struct {
	PlayerID      string
	Amount        string
	Currency      string
	CorrelationID string
}

// OpenWalletCommand is a validated wallet opening.
type OpenWalletCommand struct {
	PlayerID       uuid.UUID
	InitialBalance money.Money
	CorrelationID  string
}

// NewOpenWalletCommand validates a wallet opening request.
func NewOpenWalletCommand(in OpenWalletInput) (OpenWalletCommand, error) {
	ids, err := parseUUIDs(in.PlayerID)
	if err != nil {
		return OpenWalletCommand{}, err
	}
	balance, err := money.Parse(in.Amount, in.Currency)
	if err != nil {
		return OpenWalletCommand{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	return OpenWalletCommand{PlayerID: ids[0], InitialBalance: balance, CorrelationID: in.CorrelationID}, nil
}
