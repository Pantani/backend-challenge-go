package app

import (
	"context"
)

// InboundMessage is a decoded broker message carrying an operation.
type InboundMessage struct {
	Consumer  string
	MessageID string
	// Hash fingerprints the message content to detect a reused message id.
	Hash    string
	Command SubmitCommand
}

// ConsumeResult is the outcome of a consumed message.
type ConsumeResult struct {
	Result SubmitResult
	// Duplicate reports a redelivery already handled by the inbox.
	Duplicate bool
}

// ConsumeMessage records the message in the inbox and processes its
// operation in the same SQL transaction, so the inbox completion, the
// operation state, the balance, the ledger and the outbox records commit (or
// roll back) together. A redelivery of a committed message is a duplicate.
func (s *WagerService) ConsumeMessage(ctx context.Context, msg InboundMessage) (ConsumeResult, error) {
	start := s.Clock.Now()
	var out ConsumeResult
	err := retryConflicts(s.ConflictRetries, s.onConflict("consume"), func() error {
		return s.UoW.Do(ctx, func(ctx context.Context, r Repositories) error {
			var err error
			out, err = s.consumeInTx(ctx, r, msg)
			return err
		})
	})
	if err == nil && !out.Duplicate {
		s.Observe(SourceSQS, out.Result, start)
	}
	return out, err
}

func (s *WagerService) consumeInTx(ctx context.Context, r Repositories, msg InboundMessage) (ConsumeResult, error) {
	now := s.Clock.Now()
	entry, created, err := r.Inbox().Register(ctx, msg.Consumer, msg.MessageID, msg.Hash, now)
	if err != nil {
		return ConsumeResult{}, err
	}
	if !created {
		return duplicate(entry, msg)
	}
	res, err := s.SubmitInTx(ctx, r, msg.Command)
	if err != nil {
		return ConsumeResult{}, err
	}
	return ConsumeResult{Result: res}, r.Inbox().Complete(ctx, msg.Consumer, msg.MessageID, res.Transaction.ID(), now)
}

// duplicate verifies that a redelivered message kept its content. Inbox rows
// are only visible once committed together with their completion.
func duplicate(entry InboxEntry, msg InboundMessage) (ConsumeResult, error) {
	if entry.PayloadHash != msg.Hash {
		return ConsumeResult{}, ErrInboxConflict
	}
	return ConsumeResult{Duplicate: true}, nil
}
