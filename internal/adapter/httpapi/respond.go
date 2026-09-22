package httpapi

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Pantani/backend-challenge-go/internal/app"
	"github.com/Pantani/backend-challenge-go/internal/domain/wager"
)

// maxBodyBytes bounds request bodies.
const maxBodyBytes = 64 << 10

// Stable error codes of the HTTP contract.
const (
	CodeInvalidRequest      = "INVALID_REQUEST"
	CodeUnauthorized        = "UNAUTHORIZED"
	CodeForbidden           = "FORBIDDEN"
	CodeWalletNotFound      = "WALLET_NOT_FOUND"
	CodeTransactionNotFound = "TRANSACTION_NOT_FOUND"
	CodeWalletExists        = "WALLET_ALREADY_EXISTS"
	CodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	CodeDuplicateExternal   = "DUPLICATE_EXTERNAL_TRANSACTION"
	CodeServiceUnavailable  = "SERVICE_UNAVAILABLE"
	CodeInternalError       = "INTERNAL_ERROR"
	retryAfterSeconds       = "1"
	contentTypeJSON         = "application/json"
	headerIdempotencyKey    = "Idempotency-Key"
	headerCorrelationID     = "X-Correlation-Id"
	headerRetryAfter        = "Retry-After"
	headerWWWAuthenticate   = "WWW-Authenticate"
	headerContentType       = "Content-Type"
	bearerChallenge         = `Bearer realm="wallet-api"`                        //nolint:gosec // RFC 6750 challenge, not a credential
	bearerChallengeInvalid  = `Bearer realm="wallet-api", error="invalid_token"` //nolint:gosec // RFC 6750 challenge, not a credential
)

// errorTable maps sentinels to the contract. Messages are fixed, so wrapped
// internal context never reaches clients; the only exception is validation,
// whose detail describes the client's own input so it can be corrected.
var errorTable = []struct {
	err    error
	status int
	code   string
	msg    string
}{
	{app.ErrValidation, http.StatusBadRequest, CodeInvalidRequest, ""},
	{app.ErrForbidden, http.StatusForbidden, CodeForbidden, "operation not allowed for this client"},
	{app.ErrWalletNotFound, http.StatusNotFound, CodeWalletNotFound, "wallet not found"},
	{app.ErrTransactionNotFound, http.StatusNotFound, CodeTransactionNotFound, "transaction not found"},
	{app.ErrWalletExists, http.StatusConflict, CodeWalletExists, "a wallet already exists for this player and currency"},
	{app.ErrIdempotencyConflict, http.StatusConflict, CodeIdempotencyConflict, "idempotency key reused with a different payload"},
	{app.ErrDuplicateExternalID, http.StatusConflict, CodeDuplicateExternal, "external transaction already registered with another idempotency key"},
}

// classify maps an error to status, code and a client-safe message.
func classify(err error) (int, string, string) {
	for _, e := range errorTable {
		if errors.Is(err, e.err) {
			return e.status, e.code, cmp.Or(e.msg, err.Error())
		}
	}
	if app.IsTransient(err) {
		return http.StatusServiceUnavailable, CodeServiceUnavailable, "temporarily unavailable, retry with the same idempotency key"
	}
	return http.StatusInternalServerError, CodeInternalError, "internal error"
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set(headerRetryAfter, retryAfterSeconds)
	}
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

// decodeJSON reads exactly one JSON object with no unknown fields.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("%w: malformed JSON body: %w", app.ErrValidation, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body must contain a single JSON object", app.ErrValidation)
	}
	return nil
}

// submitStatus maps the transaction state to the HTTP contract:
// 201 processed now, 200 replayed, 202 pending reference, 422 rejected/failed.
func submitStatus(res app.SubmitResult) int {
	switch res.Transaction.Status() {
	case wager.StatusProcessed:
		if res.Replay {
			return http.StatusOK
		}
		return http.StatusCreated
	case wager.StatusPendingReference:
		return http.StatusAccepted
	default:
		return http.StatusUnprocessableEntity
	}
}
