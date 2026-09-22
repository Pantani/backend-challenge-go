// Package config loads and validates the service configuration from the
// environment. Every variable has a local default (Docker Compose ports), so
// the binary runs against the local stack with no configuration at all.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/Pantani/backend-challenge-go/internal/observability"
)

const (
	// maxSQSDuration is the longest visibility timeout SQS accepts.
	maxSQSDuration = 12 * time.Hour
	maxDuration    = time.Duration(1<<63 - 1)
)

// Database is the PostgreSQL subset of the configuration, enough for
// `wallet migrate`.
type Database struct {
	// DatabaseURL is the postgres:// connection string.
	DatabaseURL string
	// DBMaxConns caps the open connections of the pool.
	DBMaxConns int
	// DBLockTimeout bounds how long a statement waits for a row lock.
	DBLockTimeout time.Duration
	// DBStatementTimeout bounds how long a single statement may run.
	DBStatementTimeout time.Duration
}

// SQS is the AWS subset of the configuration, enough for
// `wallet provision-queues`.
type SQS struct {
	// AWSRegion is the SQS region; AWSEndpoint overrides the endpoint
	// (LocalStack locally; empty, the default, means real AWS).
	AWSRegion   string
	AWSEndpoint string
	// SQSInputQueue, SQSDLQ and SQSEventsQueue are FIFO queue names.
	SQSInputQueue  string
	SQSDLQ         string
	SQSEventsQueue string
	// SQSConsumerName is the source recorded on consumed transactions.
	SQSConsumerName string
	// SQSConsumers is how many consumer goroutines poll the input queue.
	SQSConsumers int
	// SQSMaxMessages is the batch size of one receive (1..10).
	SQSMaxMessages int
	// SQSWaitTime is the long-polling wait (0..20s).
	SQSWaitTime time.Duration
	// SQSVisibilityTimeout hides a received batch from other consumers;
	// SQSProcessTimeout bounds one message and SQSAckTimeout its broker
	// follow-up. Visibility must exceed their sum multiplied by the maximum
	// batch size because messages are handled serially.
	SQSVisibilityTimeout time.Duration
	SQSProcessTimeout    time.Duration
	SQSAckTimeout        time.Duration
	// SQSRetryBase and SQSRetryMax bound the visibility backoff of a retried
	// message.
	SQSRetryBase time.Duration
	SQSRetryMax  time.Duration
	// SQSMaxReceiveCount is the redrive policy: receives before the DLQ.
	SQSMaxReceiveCount int
	// SQSSenderProviders binds SQS SenderIds to providers
	// ("senderId=provider-a|provider-b;otherId=*").
	SQSSenderProviders string
}

// Config is the complete service configuration.
type Config struct {
	// InstanceID identifies this process: it owns the outbox leases and is
	// attached to every log record.
	InstanceID string
	// LogLevel is debug, info, warn or error (case-insensitive).
	LogLevel string
	// HTTPAddr is the listen address (":0" picks a free port).
	HTTPAddr string
	// ShutdownTimeout bounds the whole graceful stop; it must exceed
	// SQSProcessTimeout so an in-flight message can complete.
	ShutdownTimeout time.Duration
	// ReadyTimeout bounds the /health/ready dependency checks and is also
	// added to the start budget of the application.
	ReadyTimeout time.Duration
	// ConflictRetries is how many times a lost optimistic-lock race is
	// retried before failing the request (0 disables retries).
	ConflictRetries int

	Database
	SQS

	// OIDCIssuer is the expected "iss"; OIDCJWKSURL is where the signing
	// keys are fetched; OIDCAudience is the expected "aud" of this API.
	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	// PendingInterval is how often due PENDING_REFERENCE operations are
	// resolved; the delay between attempts grows from PendingBaseDelay to
	// PendingMaxDelay, up to PendingMaxAttempts, PendingBatch at a time.
	PendingInterval    time.Duration
	PendingBaseDelay   time.Duration
	PendingMaxDelay    time.Duration
	PendingMaxAttempts int
	PendingBatch       int

	// OutboxInterval is how often the relay claims OutboxBatch records.
	OutboxInterval time.Duration
	OutboxBatch    int
	// OutboxLease is how long a claim is exclusive to this instance; a
	// crashed publisher's records become claimable again after it expires.
	OutboxLease time.Duration
	// OutboxRetryBase and OutboxRetryMax bound the publication backoff.
	OutboxRetryBase time.Duration
	OutboxRetryMax  time.Duration
	// OutboxPublishTimeout bounds one broker publication.
	OutboxPublishTimeout time.Duration
	// OutboxFinalizeTimeout is shared by attempt accounting and its durable outcome.
	OutboxFinalizeTimeout time.Duration
	// OutboxMaxAttempts dead-letters an event after that many failures.
	OutboxMaxAttempts int
}

// Lookup reads a variable; os.LookupEnv in production, a map in tests.
type Lookup func(key string) (string, bool)

// MapLookup returns a Lookup backed by a map.
func MapLookup(values map[string]string) Lookup {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }

// Load reads every variable (with local defaults) and validates the result.
func Load(lookup Lookup) (Config, error) {
	r := &reader{lookup: lookup}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: r.str("INSTANCE_ID", host), LogLevel: r.str("LOG_LEVEL", "info"),
		HTTPAddr: r.str("HTTP_ADDR", ":8080"), ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 30*time.Second),
		ReadyTimeout: r.dur("READY_TIMEOUT", 2*time.Second), ConflictRetries: r.int("CONFLICT_RETRIES", 5),
		Database: r.database(), SQS: r.sqs(),

		OIDCIssuer:   r.str("OIDC_ISSUER", "http://localhost:8180/realms/wallet"),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", "http://localhost:8180/realms/wallet/protocol/openid-connect/certs"),
		OIDCAudience: r.str("OIDC_AUDIENCE", "wallet-api"),
	}
	r.loadWorkers(&c)
	return finish(r, c, c.validate)
}

// LoadDatabase reads and validates only the database variables.
func LoadDatabase(lookup Lookup) (Database, error) {
	r := &reader{lookup: lookup}
	d := r.database()
	return finish(r, d, d.validate)
}

// LoadSQS reads and validates only the AWS/SQS variables.
func LoadSQS(lookup Lookup) (SQS, error) {
	r := &reader{lookup: lookup}
	s := r.sqs()
	return finish(r, s, s.validate)
}

// finish reports the parse errors first (all of them) and validates otherwise.
func finish[T any](r *reader, v T, validate func() error) (T, error) {
	var zero T
	if err := errors.Join(r.errs...); err != nil {
		return zero, err
	}
	if err := validate(); err != nil {
		return zero, err
	}
	return v, nil
}

func (r *reader) database() Database {
	return Database{
		DatabaseURL: r.str("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"),
		DBMaxConns:  r.int("DB_MAX_CONNS", 20), DBLockTimeout: r.dur("DB_LOCK_TIMEOUT", 5*time.Second),
		DBStatementTimeout: r.dur("DB_STATEMENT_TIMEOUT", 10*time.Second),
	}
}

func (r *reader) sqs() SQS {
	return SQS{
		AWSRegion: r.str("AWS_REGION", "us-east-1"), AWSEndpoint: r.str("AWS_ENDPOINT_URL", ""),
		SQSInputQueue:   r.str("SQS_INPUT_QUEUE", "wager-transactions.fifo"),
		SQSDLQ:          r.str("SQS_DLQ", "wager-transactions-dlq.fifo"),
		SQSEventsQueue:  r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo"),
		SQSConsumerName: r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSConsumers:    r.int("SQS_CONSUMERS", 2), SQSMaxMessages: r.int("SQS_MAX_MESSAGES", 10),
		SQSWaitTime: r.dur("SQS_WAIT_TIME", 10*time.Second), SQSVisibilityTimeout: r.dur("SQS_VISIBILITY_TIMEOUT", 5*time.Minute),
		SQSProcessTimeout: r.dur("SQS_PROCESS_TIMEOUT", 20*time.Second), SQSAckTimeout: r.dur("SQS_ACK_TIMEOUT", 5*time.Second),
		SQSRetryBase: r.dur("SQS_RETRY_BASE", 2*time.Second), SQSRetryMax: r.dur("SQS_RETRY_MAX", 60*time.Second),
		SQSMaxReceiveCount: r.int("SQS_MAX_RECEIVE_COUNT", 5),
		// LocalStack reports every sender as the account id 000000000000.
		SQSSenderProviders: r.str("SQS_SENDER_PROVIDERS", "000000000000=*"),
	}
}

func (r *reader) loadWorkers(c *Config) {
	c.PendingInterval, c.PendingBaseDelay = r.dur("PENDING_INTERVAL", time.Second), r.dur("PENDING_BASE_DELAY", time.Second)
	c.PendingMaxDelay, c.PendingMaxAttempts = r.dur("PENDING_MAX_DELAY", time.Minute), r.int("PENDING_MAX_ATTEMPTS", 10)
	c.PendingBatch = r.int("PENDING_BATCH", 50)
	c.OutboxInterval, c.OutboxBatch = r.dur("OUTBOX_INTERVAL", 500*time.Millisecond), r.int("OUTBOX_BATCH", 50)
	c.OutboxLease = r.dur("OUTBOX_LEASE", 30*time.Second)
	c.OutboxRetryBase, c.OutboxRetryMax = r.dur("OUTBOX_RETRY_BASE", time.Second), r.dur("OUTBOX_RETRY_MAX", time.Minute)
	c.OutboxPublishTimeout = r.dur("OUTBOX_PUBLISH_TIMEOUT", 10*time.Second)
	c.OutboxFinalizeTimeout = r.dur("OUTBOX_FINALIZE_TIMEOUT", 5*time.Second)
	c.OutboxMaxAttempts = r.int("OUTBOX_MAX_ATTEMPTS", 20)
}

// rule is one validation: ok is false when msg applies.
type rule struct {
	ok  bool
	msg string
}

// check joins the messages of every failed rule.
func check(rules []rule) error {
	var errs []error
	for _, rule := range rules {
		if !rule.ok {
			errs = append(errs, errors.New(rule.msg))
		}
	}
	return errors.Join(errs...)
}

func (d Database) validate() error {
	return check([]rule{
		{d.DatabaseURL != "", "DATABASE_URL is required"},
		{d.DBMaxConns > 0, "DB_MAX_CONNS must be positive"},
		{positiveDurations(d.DBLockTimeout, d.DBStatementTimeout), "DB_LOCK_TIMEOUT and DB_STATEMENT_TIMEOUT must be positive"},
	})
}

func (s SQS) validate() error {
	budget, budgetOK := batchBudget(s.SQSMaxMessages, s.SQSProcessTimeout, s.SQSAckTimeout)
	return check([]rule{
		{positive(s.SQSConsumers, s.SQSMaxReceiveCount), "SQS_CONSUMERS and SQS_MAX_RECEIVE_COUNT must be positive"},
		{between(s.SQSMaxMessages, 1, 10), "SQS_MAX_MESSAGES must be between 1 and 10"},
		{between(int(s.SQSWaitTime), 0, int(20*time.Second)), "SQS_WAIT_TIME must be between 0s and 20s"},
		{positiveDurations(s.SQSVisibilityTimeout, s.SQSProcessTimeout, s.SQSAckTimeout, s.SQSRetryBase, s.SQSRetryMax),
			"SQS visibility, process, ack and retry durations must be positive"},
		{budgetOK && s.SQSVisibilityTimeout > budget,
			"SQS_VISIBILITY_TIMEOUT must exceed the whole receive batch budget: SQS_MAX_MESSAGES * (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)"},
		{s.SQSRetryBase <= s.SQSRetryMax, "SQS_RETRY_BASE must not exceed SQS_RETRY_MAX"},
		{s.SQSVisibilityTimeout <= maxSQSDuration && s.SQSRetryMax <= maxSQSDuration,
			"SQS_VISIBILITY_TIMEOUT and SQS_RETRY_MAX must not exceed 12h"},
	})
}

// batchBudget returns the worst-case serial processing and acknowledgement
// time for one receive without allowing time.Duration arithmetic to wrap.
func batchBudget(maxMessages int, process, ack time.Duration) (time.Duration, bool) {
	if maxMessages <= 0 || process <= 0 || ack <= 0 {
		return 0, false
	}
	if process > maxDuration-ack {
		return 0, false
	}
	perMessage := process + ack
	if perMessage > maxDuration/time.Duration(maxMessages) {
		return 0, false
	}
	return time.Duration(maxMessages) * perMessage, true
}

// validate enforces relationships between settings.
func (c Config) validate() error {
	_, levelErr := observability.ParseLevel(c.LogLevel)
	return errors.Join(c.Database.validate(), c.SQS.validate(), check([]rule{
		{nonEmpty(c.OIDCIssuer, c.OIDCJWKSURL, c.OIDCAudience), "OIDC_ISSUER, OIDC_JWKS_URL and OIDC_AUDIENCE are required"},
		{levelErr == nil, "LOG_LEVEL must be debug, info, warn or error"},
		{c.ConflictRetries >= 0, "CONFLICT_RETRIES must not be negative"},
		{positive(c.OutboxBatch, c.PendingBatch, c.PendingMaxAttempts, c.OutboxMaxAttempts),
			"batch sizes and attempts must be positive"},
		{between(int(c.PendingBaseDelay), 1, int(c.PendingMaxDelay)), "PENDING_BASE_DELAY must be in (0, PENDING_MAX_DELAY]"},
		{positiveDurations(c.PendingInterval, c.OutboxInterval, c.OutboxLease, c.OutboxRetryBase, c.OutboxRetryMax,
			c.OutboxPublishTimeout, c.OutboxFinalizeTimeout, c.ShutdownTimeout, c.ReadyTimeout),
			"worker intervals, leases, retries and timeouts must be positive"},
		{c.OutboxRetryBase <= c.OutboxRetryMax, "OUTBOX_RETRY_BASE must not exceed OUTBOX_RETRY_MAX"},
		{outboxBudgetFits(c.OutboxPublishTimeout, c.OutboxFinalizeTimeout, c.OutboxLease),
			"OUTBOX_PUBLISH_TIMEOUT plus OUTBOX_FINALIZE_TIMEOUT must be lower than OUTBOX_LEASE"},
		{c.ShutdownTimeout > c.SQSProcessTimeout+c.SQSAckTimeout, "SHUTDOWN_TIMEOUT must exceed SQS_PROCESS_TIMEOUT plus SQS_ACK_TIMEOUT"},
		{c.ShutdownTimeout > c.OutboxPublishTimeout, "SHUTDOWN_TIMEOUT must exceed OUTBOX_PUBLISH_TIMEOUT"},
	}))
}

// outboxBudgetFits validates the serial publication and finalization budget
// using subtraction so time.Duration addition cannot overflow.
func outboxBudgetFits(publish, finalize, lease time.Duration) bool {
	return publish > 0 && finalize > 0 && lease > 0 && publish < lease && finalize < lease-publish
}

func nonEmpty(values ...string) bool {
	return !slices.Contains(values, "")
}

func positive(values ...int) bool {
	return !slices.ContainsFunc(values, func(v int) bool { return v <= 0 })
}

func positiveDurations(values ...time.Duration) bool {
	return !slices.ContainsFunc(values, func(d time.Duration) bool { return d <= 0 })
}

func between(v, lo, hi int) bool { return v >= lo && v <= hi }

// reader accumulates parse errors so every problem is reported at once.
type reader struct {
	lookup Lookup
	errs   []error
}

// str returns the variable, or def when it is unset or empty.
func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

// int parses the variable as an integer, recording a parse error.
func (r *reader) int(key string, def int) int {
	v, err := strconv.Atoi(r.str(key, strconv.Itoa(def)))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}

// dur parses the variable as a Go duration ("500ms", "30s"), recording a parse error.
func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(r.str(key, def.String()))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}
