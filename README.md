# Wallet Service — processamento distribuído de apostas em Go

Serviço em Go 1.27 + Uber Fx que movimenta carteiras de jogadores a partir de uma API HTTP e de um consumidor SQS, com as mesmas garantias nas duas entradas: dinheiro exato, ledger append-only, idempotência persistente, coordenação por carteira entre várias instâncias, inbox/outbox transacionais e recuperação após falhas.

- Enunciado original: [`docs/CHALLENGE.md`](docs/CHALLENGE.md)
- Decisões técnicas, limitações e interpretações: [`ARCHITECTURE.md`](ARCHITECTURE.md)

## Pré-requisitos

| Ferramenta | Versão usada |
| --- | --- |
| Go | 1.27.1 (declarada em `go.mod` e no `Dockerfile`) |
| Docker + Docker Compose v2 | Docker 29 |
| golangci-lint (opcional, para `make lint`) | v2 |

As portas locais usadas são `5432` (PostgreSQL), `4566` (LocalStack), `8180` (Keycloak) e `8080`–`8082` (três instâncias do serviço).

## Subir tudo

```sh
docker compose up --build
```

O compose sobe, nesta ordem:

1. `postgres`, `localstack` (SQS) e `keycloak` (realm `wallet` importado de `deploy/keycloak/realm-wallet.json`);
2. `migrate`: executa `wallet migrate up`;
3. `provision-queues`: executa `wallet provision-queues`, que cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (com redrive `maxReceiveCount=5`) e `wallet-events.fifo` (destino da outbox);
4. `app-1`, `app-2` e `app-3`: três processos independentes do mesmo serviço, em `localhost:8080`, `:8081` e `:8082`.

`make up`, `make down` e `make logs` são atalhos.

### Migrations

As migrations ficam versionadas em `internal/adapter/postgres/migrations` (golang-migrate, embutidas no binário):

```sh
wallet migrate up          # aplica todas
wallet migrate down [n]    # reverte n (padrão 1)
wallet migrate version     # versão atual
```

Com o compose: `make migrate-up` / `make migrate-down`. Fora do Docker: `go run ./cmd/wallet migrate up`, com `DATABASE_URL` apontando para o banco.

### Filas

`wallet provision-queues` cria as filas de forma idempotente (`make provision-queues`). No compose isso roda automaticamente.

## Variáveis de ambiente

Todas têm padrão local. Veja [`.env.example`](.env.example), que o compose carrega. As principais:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `DATABASE_URL` | `postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable` | PostgreSQL |
| `DB_LOCK_TIMEOUT` / `DB_STATEMENT_TIMEOUT` | `5s` / `10s` | espera máxima por lock de carteira / por comando |
| `OIDC_ISSUER` | `http://localhost:8180/realms/wallet` | `iss` esperado nos tokens |
| `OIDC_JWKS_URL` | `…/protocol/openid-connect/certs` | onde buscar as chaves (no compose, `http://keycloak:8080/…`) |
| `OIDC_AUDIENCE` | `wallet-api` | `aud` exigido |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | LocalStack | cliente SQS |
| `SQS_INPUT_QUEUE`, `SQS_DLQ`, `SQS_EVENTS_QUEUE` | nomes acima | filas |
| `SQS_CONSUMERS`, `SQS_VISIBILITY_TIMEOUT`, `SQS_PROCESS_TIMEOUT`, `SQS_RETRY_BASE/MAX`, `SQS_MAX_RECEIVE_COUNT` | `2`, `30s`, `20s`, `2s/60s`, `5` | consumidor |
| `PENDING_*` | `1s` base, `60s` máx., `10` tentativas | referências pendentes |
| `OUTBOX_*` | `500ms`, lote `50`, lease `30s` | publisher da outbox |
| `SHUTDOWN_TIMEOUT` | `25s` | prazo do encerramento |

A configuração é validada na inicialização (valores inválidos e relações como `SQS_PROCESS_TIMEOUT < SQS_VISIBILITY_TIMEOUT`). O start também falha se PostgreSQL, SQS ou as filas estiverem indisponíveis.

## Autenticação

O Keycloak é provisionado automaticamente com clients `client_credentials`:

| Client | Secret | Papel | `provider_id` |
| --- | --- | --- | --- |
| `wallet-service` | `wallet-service-secret` | `wallet-admin` (serviço interno) | — |
| `provider-a` | `provider-a-secret` | `wager-provider` | `provider-a` |
| `provider-b` | `provider-b-secret` | `wager-provider` | `provider-b` |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wager-provider` (token de 3 s, para testes de expiração) | `provider-a` |
| `no-role-client` | `no-role-client-secret` | nenhum | — |

```sh
token() {
  curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
    http://localhost:8180/realms/wallet/protocol/openid-connect/token | jq -r .access_token
}
ADMIN=$(token wallet-service)
PROVIDER_A=$(token provider-a)
```

## Exemplos de chamadas

```sh
# Abrir carteira (somente o serviço interno)
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

WALLET=<id retornado>

# Aposta (provedor), enviada a outra instância
curl -s -X POST localhost:8081/wagering/transactions -H "Authorization: Bearer $PROVIDER_A" \
  -H 'Idempotency-Key: provider-a:transaction-123' -H 'Content-Type: application/json' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
# => 201 {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
# Repetir a mesma chamada => 200 com "idempotentReplay":true e o mesmo saldo original.

# Reversão: acrescente "referenceExternalTransactionId" e use kind REFUND ou ROLLBACK.

# Consultas
curl -s localhost:8082/wallets/$WALLET -H "Authorization: Bearer $ADMIN"
curl -s "localhost:8082/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $ADMIN"
curl -s localhost:8082/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER_A"
curl -s -X POST localhost:8082/wallets/$WALLET/reconciliation -H "Authorization: Bearer $ADMIN"

# Health e métricas (públicos)
curl -s localhost:8080/health/live
curl -s localhost:8080/health/ready
curl -s localhost:8080/metrics

# Operação via SQS
awslocal sqs send-message --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id $WALLET --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-124","idempotencyKey":"provider-a:transaction-124","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"'$WALLET'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}'
```

Os códigos HTTP, os corpos de erro e os `failureCode` estão em [`ARCHITECTURE.md`](ARCHITECTURE.md#contrato-http).

## Testes

```sh
go test ./...          # unitários (sem Docker)
go test -race ./...
go vet ./...
make lint              # gofmt/goimports, gocyclo ≤ 6, gocognit ≤ 8 (código e testes)
```

### Integração e e2e

As dependências dos testes sobem sozinhas via **testcontainers** (é preciso Docker rodando; as imagens são as mesmas do compose). As suítes são separadas por build tags:

```sh
# PostgreSQL, LocalStack e Keycloak reais: schema/constraints/imutabilidade, migrations up/down,
# concorrência (50 apostas iguais, 80+80 sobre 100, carteiras independentes, reversões concorrentes),
# lock timeout, inbox/reentrega após crash, DLQ/redrive, outbox com publishers concorrentes e
# recuperação, autenticação real (inválida, adulterada, expirada), isolamento entre provedores,
# composição Fx (start/stop, liberação dos workers, falhas de dependência) e CLI.
make test-integration   # go test -race -count=1 -tags integration ./test/integration/...

# O binário compilado roda como 3 processos independentes: duplicidade e disputa entre instâncias,
# HTTP + SQS para a mesma operação, REFUND antes da BET, SIGKILL das três instâncias com reinício
# (idempotência e pendências preservadas) e reconciliação de todas as carteiras ao final.
make test-e2e           # go test -count=1 -tags e2e ./test/e2e/...

# Cobertura combinada unidade + integração + e2e (binário compilado com -cover).
make coverage
```

Resultado atual de `make coverage`: **100,0%** das instruções de `cmd/` e `internal/`.

### Simulações de falha cobertas

| Situação | Onde |
| --- | --- |
| Consumidor morre depois do commit e antes do `DeleteMessage` | `TestConsumerCrashAfterCommitIsRedeliveredAndDeduplicated` |
| Publisher morre entre publicar e confirmar na outbox | `TestCrashBetweenPublishAndConfirmIsRecovered` |
| Commit feito, publicação nunca feita (outbox pendente) | `TestTwoPublishersShareTheOutbox` |
| PostgreSQL indisponível no consumo (retry + redrive para a DLQ) | `TestTransientFailuresAreRetriedThenRedrivenToTheDLQ` |
| Lock de carteira preso (lock timeout → retry/503) | `TestLockTimeoutIsTransient` |
| Encerramento abrupto de todas as instâncias | `TestZZCrashAndRestart` (e2e) |
| `SIGTERM`: para de buscar trabalho, conclui ou libera o em andamento | `TestConsumerReleasesUnstartedMessagesOnShutdown`, `TestApplicationStopsWorkersOnShutdown` |
