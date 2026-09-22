// Package observability provides JSON logging with contextual identifiers
// and the Prometheus metrics of the service.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

type ctxKey struct{}

// WithAttrs returns a context whose log records carry the given attributes
// (correlationId, messageId, transactionId, walletId, providerId...).
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	existing, _ := ctx.Value(ctxKey{}).([]slog.Attr)
	merged := make([]slog.Attr, 0, len(existing)+len(attrs))
	merged = append(merged, existing...)
	merged = append(merged, attrs...)
	return context.WithValue(ctx, ctxKey{}, merged)
}

// contextHandler adds the attributes stored in the context to each record.
type contextHandler struct {
	slog.Handler
}

// Handle implements slog.Handler.
func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if attrs, ok := ctx.Value(ctxKey{}).([]slog.Attr); ok {
		r.AddAttrs(attrs...)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// NewLogger builds a JSON logger writing to w.
func NewLogger(w io.Writer, level string, instance string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(contextHandler{h}).With("service", "wallet-service", "instance", instance)
}
