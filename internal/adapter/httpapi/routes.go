package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/observability"
)

func pathUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s must be a UUID", app.ErrValidation, name)
	}
	return id, nil
}

func (h *handler) openWallet(w http.ResponseWriter, r *http.Request) {
	var body openWalletRequest
	if err := decodeJSON(w, r, &body); err != nil {
		h.fail(w, r, err)
		return
	}
	cmd, err := app.NewOpenWalletCommand(app.OpenWalletInput{
		PlayerID: body.PlayerID, Amount: body.InitialBalance.Amount, Currency: body.InitialBalance.Currency,
		CorrelationID: correlationFrom(r.Context()),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	wal, err := h.Wallets.Open(r.Context(), cmd)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, newWalletResponse(wal))
}

func (h *handler) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	wal, err := h.Wallets.Get(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newWalletResponse(wal))
}

func (h *handler) getLedger(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	limit, err := queryLimit(r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	page, err := h.Wallets.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newLedgerResponse(id, page))
}

func queryLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: limit must be an integer", app.ErrValidation)
	}
	return limit, nil
}

func (h *handler) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "walletId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	rec, err := h.Wallets.Reconcile(r.Context(), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newReconciliationResponse(rec))
}

// submit handles POST /wagering/transactions. The providerId of the body
// must be the one bound to the token: a provider cannot act for another.
func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	cmd, err := h.submitCommand(w, r)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	ctx := observability.WithAttrs(r.Context(), slog.String("walletId", cmd.WalletID.String()))
	res, err := h.Wagers.Submit(ctx, cmd)
	if err != nil {
		h.fail(w, r.WithContext(ctx), err)
		return
	}
	h.Logger.InfoContext(ctx, "wager transaction submitted", "transactionId", res.Transaction.ID(),
		"status", res.Transaction.Status(), "idempotentReplay", res.Replay)
	writeJSON(w, submitStatus(res), newSubmitResponse(res))
}

func (h *handler) submitCommand(w http.ResponseWriter, r *http.Request) (app.SubmitCommand, error) {
	var body submitRequest
	if err := decodeJSON(w, r, &body); err != nil {
		return app.SubmitCommand{}, err
	}
	key := r.Header.Get(headerIdempotencyKey)
	if key == "" {
		return app.SubmitCommand{}, fmt.Errorf("%w: %s header is required", app.ErrValidation, headerIdempotencyKey)
	}
	if body.ProviderID != principalFrom(r.Context()).ProviderID {
		return app.SubmitCommand{}, fmt.Errorf("%w: providerId does not match the authenticated provider", app.ErrForbidden)
	}
	return app.NewSubmitCommand(body.toInput(key, correlationFrom(r.Context())))
}

func callerFrom(r *http.Request) app.Caller {
	p := principalFrom(r.Context())
	return app.Caller{ProviderID: p.ProviderID, Internal: p.IsInternal()}
}

func (h *handler) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := pathUUID(r, "transactionId")
	if err != nil {
		h.fail(w, r, err)
		return
	}
	t, err := h.Wagers.Get(r.Context(), callerFrom(r), id)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(t))
}

func (h *handler) getTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	t, err := h.Wagers.GetByExternal(r.Context(), callerFrom(r), r.PathValue("providerId"), r.PathValue("externalTransactionId"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionResponse(t))
}
