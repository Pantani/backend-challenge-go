package httpapi

import (
	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/event"
	"github.com/Pantani/backend-challenge-go/internal/domain/money"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
	"github.com/Pantani/backend-challenge-go/internal/domain/wallet"
)

// moneyDTO is the external money contract: amount is always a JSON string,
// so a JSON number (a float on most clients) is rejected by the decoder.
type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toMoneyDTO(m money.Money) *moneyDTO {
	if m.Validate() != nil {
		return nil
	}
	return &moneyDTO{Amount: m.Amount(), Currency: string(m.Currency())}
}

type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        string    `json:"id"`
	PlayerID  string    `json:"playerId"`
	Balance   *moneyDTO `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt string    `json:"createdAt"`
	UpdatedAt string    `json:"updatedAt"`
}

func newWalletResponse(w *wallet.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID().String(), PlayerID: w.PlayerID().String(), Balance: toMoneyDTO(w.Balance()),
		Version: w.Version(), CreatedAt: event.FormatTime(w.CreatedAt()), UpdatedAt: event.FormatTime(w.UpdatedAt()),
	}
}

type ledgerEntryResponse struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         *moneyDTO `json:"money"`
	BalanceBefore *moneyDTO `json:"balanceBefore"`
	BalanceAfter  *moneyDTO `json:"balanceAfter"`
	CreatedAt     string    `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Items      []ledgerEntryResponse `json:"items"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func newLedgerResponse(walletID uuid.UUID, page app.LedgerPage) ledgerResponse {
	items := make([]ledgerEntryResponse, 0, len(page.Entries))
	for _, e := range page.Entries {
		items = append(items, ledgerEntryResponse{
			ID: e.ID().String(), TransactionID: e.TransactionID().String(), Direction: string(e.Direction()),
			Money: toMoneyDTO(e.Amount()), BalanceBefore: toMoneyDTO(e.BalanceBefore()),
			BalanceAfter: toMoneyDTO(e.BalanceAfter()), CreatedAt: event.FormatTime(e.CreatedAt()),
		})
	}
	return ledgerResponse{WalletID: walletID.String(), Items: items, NextCursor: page.NextCursor}
}

type reconciliationResponse struct {
	WalletID          string    `json:"walletId"`
	StoredBalance     *moneyDTO `json:"storedBalance"`
	CalculatedBalance *moneyDTO `json:"calculatedBalance"`
	Difference        *moneyDTO `json:"difference"`
	Consistent        bool      `json:"consistent"`
	CheckedEntries    int64     `json:"checkedEntries"`
}

func newReconciliationResponse(r app.Reconciliation) reconciliationResponse {
	return reconciliationResponse{
		WalletID: r.WalletID.String(), StoredBalance: toMoneyDTO(r.Stored), CalculatedBalance: toMoneyDTO(r.Calculated),
		Difference: toMoneyDTO(r.Difference), Consistent: r.Consistent, CheckedEntries: r.CheckedEntries,
	}
}

type submitRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId"`
}

func (r submitRequest) toInput(idempotencyKey, correlationID string) app.SubmitInput {
	return app.SubmitInput{
		ProviderID: r.ProviderID, ExternalTransactionID: r.ExternalTransactionID, IdempotencyKey: idempotencyKey,
		PlayerID: r.PlayerID, WalletID: r.WalletID, RoundID: r.RoundID, GameID: r.GameID, Kind: r.Kind,
		Amount: r.Money.Amount, Currency: r.Money.Currency,
		ReferenceExternalTransactionID: r.ReferenceExternalTransactionID, CorrelationID: correlationID,
	}
}

type submitResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      string    `json:"failureCode,omitempty"`
	NextAttemptAt    string    `json:"nextAttemptAt,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

func newSubmitResponse(res app.SubmitResult) submitResponse {
	t := res.Transaction
	return submitResponse{
		TransactionID: t.ID().String(), Status: string(t.Status()), Balance: toMoneyDTO(t.ResultBalance()),
		FailureCode: string(t.FailureCode()), NextAttemptAt: optionalTime(t), IdempotentReplay: res.Replay,
	}
}

func optionalTime(t *wager.Transaction) string {
	if t.Status() != wager.StatusPendingReference {
		return ""
	}
	return event.FormatTime(t.NextAttemptAt())
}

type transactionResponse struct {
	TransactionID                  string    `json:"transactionId"`
	Origin                         string    `json:"origin"`
	Kind                           string    `json:"kind"`
	Status                         string    `json:"status"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	Money                          *moneyDTO `json:"money"`
	ProviderID                     string    `json:"providerId,omitempty"`
	ExternalTransactionID          string    `json:"externalTransactionId,omitempty"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string    `json:"referenceTransactionId,omitempty"`
	FailureCode                    string    `json:"failureCode,omitempty"`
	Balance                        *moneyDTO `json:"balance,omitempty"`
	Attempts                       int       `json:"attempts"`
	NextAttemptAt                  string    `json:"nextAttemptAt,omitempty"`
	CreatedAt                      string    `json:"createdAt"`
	UpdatedAt                      string    `json:"updatedAt"`
}

func newTransactionResponse(t *wager.Transaction) transactionResponse {
	ext := t.External()
	resp := transactionResponse{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()), Status: string(t.Status()),
		WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(), Money: toMoneyDTO(t.Amount()),
		ProviderID: ext.ProviderID, ExternalTransactionID: ext.ExternalID, RoundID: ext.RoundID, GameID: ext.GameID,
		ReferenceExternalTransactionID: ext.ReferenceExternalID, FailureCode: string(t.FailureCode()),
		Balance: toMoneyDTO(t.ResultBalance()), Attempts: t.Attempts(), NextAttemptAt: optionalTime(t),
		CreatedAt: event.FormatTime(t.CreatedAt()), UpdatedAt: event.FormatTime(t.UpdatedAt()),
	}
	if t.ReferenceTxID() != uuid.Nil {
		resp.ReferenceTransactionID = t.ReferenceTxID().String()
	}
	return resp
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
