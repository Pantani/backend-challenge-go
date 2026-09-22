// Package config loads and validates the service configuration from the
// environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"
)

// Config is the complete service configuration.
type Config struct {
	InstanceID      string
	LogLevel        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
	ReadyTimeout    time.Duration
	ConflictRetries int

	DatabaseURL        string
	DBMaxConns         int
	DBLockTimeout      time.Duration
	DBStatementTimeout time.Duration

	OIDCIssuer   string
	OIDCJWKSURL  string
	OIDCAudience string

	AWSRegion         string
	AWSEndpoint       string
	SQSInputQueue     string
	SQSDLQ            string
	SQSEventsQueue    string
	SQSConsumerName   string
	SQSConsumers      int
	SQSMaxMessages    int
	SQSWaitTime       time.Duration
	SQSVisibility     time.Duration
	SQSProcessTimeout time.Duration
	SQSRetryBase      time.Duration
	SQSRetryMax       time.Duration
	SQSMaxReceive     int
	// SQSSenderProviders binds SQS SenderIds to providers
	// ("senderId=provider-a|provider-b;otherId=*").
	SQSSenderProviders string

	PendingInterval    time.Duration
	PendingBaseDelay   time.Duration
	PendingMaxDelay    time.Duration
	PendingMaxAttempts int
	PendingBatch       int

	OutboxInterval  time.Duration
	OutboxBatch     int
	OutboxLease     time.Duration
	OutboxRetryBase time.Duration
	OutboxRetryMax  time.Duration
	OutboxMaxTries  int
}

// Lookup reads a variable; os.LookupEnv in production, a map in tests.
type Lookup func(key string) (string, bool)

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }

// Load reads every variable (with local defaults) and validates the result.
func Load(lookup Lookup) (Config, error) {
	r := reader{lookup: lookup}
	host, _ := os.Hostname()
	c := Config{
		InstanceID: r.str("INSTANCE_ID", host), LogLevel: r.str("LOG_LEVEL", "info"),
		HTTPAddr: r.str("HTTP_ADDR", ":8080"), ShutdownTimeout: r.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
		ReadyTimeout: r.dur("READY_TIMEOUT", 2*time.Second), ConflictRetries: r.int("CONFLICT_RETRIES", 5),

		DatabaseURL: r.str("DATABASE_URL", "postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable"),
		DBMaxConns:  r.int("DB_MAX_CONNS", 20), DBLockTimeout: r.dur("DB_LOCK_TIMEOUT", 5*time.Second),
		DBStatementTimeout: r.dur("DB_STATEMENT_TIMEOUT", 10*time.Second),

		OIDCIssuer:   r.str("OIDC_ISSUER", "http://localhost:8081/realms/wallet"),
		OIDCJWKSURL:  r.str("OIDC_JWKS_URL", "http://localhost:8081/realms/wallet/protocol/openid-connect/certs"),
		OIDCAudience: r.str("OIDC_AUDIENCE", "wallet-api"),
	}
	r.loadSQS(&c)
	r.loadWorkers(&c)
	if err := errors.Join(r.errs...); err != nil {
		return Config{}, err
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (r *reader) loadSQS(c *Config) {
	c.AWSRegion, c.AWSEndpoint = r.str("AWS_REGION", "us-east-1"), r.str("AWS_ENDPOINT_URL", "")
	c.SQSInputQueue = r.str("SQS_INPUT_QUEUE", "wager-transactions.fifo")
	c.SQSDLQ = r.str("SQS_DLQ", "wager-transactions-dlq.fifo")
	c.SQSEventsQueue = r.str("SQS_EVENTS_QUEUE", "wallet-events.fifo")
	c.SQSConsumerName = r.str("SQS_CONSUMER_NAME", "wager-transactions-consumer")
	c.SQSConsumers, c.SQSMaxMessages = r.int("SQS_CONSUMERS", 2), r.int("SQS_MAX_MESSAGES", 10)
	c.SQSWaitTime, c.SQSVisibility = r.dur("SQS_WAIT_TIME", 10*time.Second), r.dur("SQS_VISIBILITY_TIMEOUT", 30*time.Second)
	c.SQSProcessTimeout = r.dur("SQS_PROCESS_TIMEOUT", 20*time.Second)
	c.SQSRetryBase, c.SQSRetryMax = r.dur("SQS_RETRY_BASE", 2*time.Second), r.dur("SQS_RETRY_MAX", 60*time.Second)
	c.SQSMaxReceive = r.int("SQS_MAX_RECEIVE_COUNT", 5)
	// LocalStack reports every sender as the account id 000000000000.
	c.SQSSenderProviders = r.str("SQS_SENDER_PROVIDERS", "000000000000=*")
}

func (r *reader) loadWorkers(c *Config) {
	c.PendingInterval, c.PendingBaseDelay = r.dur("PENDING_INTERVAL", time.Second), r.dur("PENDING_BASE_DELAY", time.Second)
	c.PendingMaxDelay, c.PendingMaxAttempts = r.dur("PENDING_MAX_DELAY", time.Minute), r.int("PENDING_MAX_ATTEMPTS", 10)
	c.PendingBatch = r.int("PENDING_BATCH", 50)
	c.OutboxInterval, c.OutboxBatch = r.dur("OUTBOX_INTERVAL", 500*time.Millisecond), r.int("OUTBOX_BATCH", 50)
	c.OutboxLease = r.dur("OUTBOX_LEASE", 30*time.Second)
	c.OutboxRetryBase, c.OutboxRetryMax = r.dur("OUTBOX_RETRY_BASE", time.Second), r.dur("OUTBOX_RETRY_MAX", time.Minute)
	c.OutboxMaxTries = r.int("OUTBOX_MAX_ATTEMPTS", 20)
}

// validate enforces relationships between settings.
func (c Config) validate() error {
	rules := []struct {
		ok  bool
		msg string
	}{
		{nonEmpty(c.DatabaseURL, c.OIDCIssuer, c.OIDCJWKSURL, c.OIDCAudience), "DATABASE_URL, OIDC_ISSUER, OIDC_JWKS_URL and OIDC_AUDIENCE are required"},
		{positive(c.DBMaxConns, c.SQSConsumers, c.OutboxBatch, c.PendingBatch, c.PendingMaxAttempts, c.OutboxMaxTries, c.SQSMaxReceive),
			"pool size, consumers, batch sizes and attempts must be positive"},
		{between(c.SQSMaxMessages, 1, 10), "SQS_MAX_MESSAGES must be between 1 and 10"},
		{between(int(c.SQSWaitTime), 0, int(20*time.Second)), "SQS_WAIT_TIME must be between 0s and 20s"},
		{c.SQSProcessTimeout < c.SQSVisibility, "SQS_PROCESS_TIMEOUT must be lower than SQS_VISIBILITY_TIMEOUT"},
		{between(int(c.PendingBaseDelay), 1, int(c.PendingMaxDelay)), "PENDING_BASE_DELAY must be in (0, PENDING_MAX_DELAY]"},
		{positiveDurations(c.PendingInterval, c.OutboxInterval, c.OutboxLease, c.OutboxRetryBase, c.OutboxRetryMax,
			c.SQSRetryBase, c.SQSRetryMax, c.SQSProcessTimeout, c.ShutdownTimeout, c.ReadyTimeout, c.DBLockTimeout,
			c.DBStatementTimeout), "worker intervals, leases, retries and timeouts must be positive"},
	}
	var errs []error
	for _, rule := range rules {
		if !rule.ok {
			errs = append(errs, errors.New(rule.msg))
		}
	}
	return errors.Join(errs...)
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

func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok && v != "" {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v, err := strconv.Atoi(r.str(key, strconv.Itoa(def)))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(r.str(key, def.String()))
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %w", key, err))
	}
	return v
}
