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
3. `provision-queues` (em paralelo com `migrate`): executa `wallet provision-queues`, que cria `wager-transactions.fifo`, `wager-transactions-dlq.fifo` (com redrive `maxReceiveCount=5`) e `wallet-events.fifo` (destino da outbox);
4. `app-1`, `app-2` e `app-3`: três processos independentes do mesmo serviço, em `localhost:8080`, `:8081` e `:8082`.

`make up`, `make down` (para os containers e preserva os volumes), `make clean` (remove também os volumes) e `make logs` são atalhos.

### Migrations

As migrations ficam versionadas em `internal/adapter/postgres/migrations` (golang-migrate, embutidas no binário):

```sh
wallet migrate up          # aplica todas
wallet migrate down [n]    # reverte n (padrão 1)
wallet migrate version     # versão atual
```

Com o compose: `make migrate-up` / `make migrate-down`. Fora do Docker: `go run ./cmd/wallet migrate up`, com `DATABASE_URL` apontando para o banco. Os comandos `migrate` e `provision-queues` validam apenas as variáveis que usam (banco e SQS, respectivamente); só `serve` exige a configuração completa.

O binário devolve `0` em sucesso, `2` para comando ou argumentos inválidos (imprime o uso) e `1` para qualquer outra falha; erros vão para `stderr`, os logs JSON para `stdout`.

### Filas

`wallet provision-queues` cria as filas de forma idempotente (`make provision-queues`). Rodar de novo depois de mudar `SQS_MAX_RECEIVE_COUNT` ou `SQS_VISIBILITY_TIMEOUT` reconcilia as filas existentes. No compose isso roda automaticamente.

## Variáveis de ambiente

Todas têm padrão local, exceto `AWS_ENDPOINT_URL`, que vazio significa a AWS real (a lista completa está em [`.env.example`](.env.example), que o compose carrega). As principais:

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | identifica o processo (dono dos leases da outbox e atributo `instance` dos logs) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` ou `error` |
| `DATABASE_URL` | `postgres://wallet:wallet@localhost:5432/wallet?sslmode=disable` | PostgreSQL |
| `DB_LOCK_TIMEOUT` / `DB_STATEMENT_TIMEOUT` | `5s` / `10s` | espera máxima por lock de carteira / por comando |
| `OIDC_ISSUER` | `http://localhost:8180/realms/wallet` | `iss` esperado nos tokens |
| `OIDC_JWKS_URL` | `…/protocol/openid-connect/certs` | onde buscar as chaves (no compose, `http://keycloak:8080/…`) |
| `OIDC_AUDIENCE` | `wallet-api` | `aud` exigido |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | vazio (AWS real; o `.env.example` aponta para o LocalStack), `us-east-1` | cliente SQS |
| `SQS_INPUT_QUEUE`, `SQS_DLQ`, `SQS_EVENTS_QUEUE` | nomes acima | filas |
| `SQS_SENDER_PROVIDERS` | `000000000000=*` | vínculo `SenderId` do SQS → provedores permitidos (`id=provider-a\|provider-b;outroId=*`) |
| `SQS_CONSUMERS`, `SQS_MAX_MESSAGES`, `SQS_WAIT_TIME`, `SQS_VISIBILITY_TIMEOUT`, `SQS_PROCESS_TIMEOUT`, `SQS_ACK_TIMEOUT`, `SQS_RETRY_BASE/MAX`, `SQS_MAX_RECEIVE_COUNT` | `2`, `10`, `10s`, `5m`, `20s`, `5s`, `2s/60s`, `5` | consumer; visibility covers the worst-case serial budget of the whole batch |
| `PENDING_INTERVAL`, `PENDING_BASE_DELAY`, `PENDING_MAX_DELAY`, `PENDING_MAX_ATTEMPTS`, `PENDING_BATCH` | `1s`, `1s`, `60s`, `10`, `50` | referências pendentes |
| `OUTBOX_INTERVAL`, `OUTBOX_BATCH`, `OUTBOX_LEASE`, `OUTBOX_RETRY_BASE/MAX`, `OUTBOX_PUBLISH_TIMEOUT`, `OUTBOX_FINALIZE_TIMEOUT`, `OUTBOX_MAX_ATTEMPTS` | `500ms`, `50`, `30s`, `1s/60s`, `10s`, `5s`, `20` | outbox publisher; attempt accounting and its terminal mutation share the total finalization budget |
| `CONFLICT_RETRIES` | `5` | novas tentativas de uma transação SQL que perdeu uma disputa |
| `SHUTDOWN_TIMEOUT` | `30s` | prazo do encerramento (maior que `SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT` e que `OUTBOX_PUBLISH_TIMEOUT`) |

Configuration is validated at startup: invalid values, an unknown `LOG_LEVEL`, non-positive intervals and timeouts, `SQS_MAX_MESSAGES` outside 1–10, `SQS_WAIT_TIME` above 20s, `SQS_VISIBILITY_TIMEOUT` or `SQS_RETRY_MAX` above the SQS 12h limit, `*_RETRY_BASE > *_RETRY_MAX`, `PENDING_BASE_DELAY` outside `(0, PENDING_MAX_DELAY]`, `SQS_VISIBILITY_TIMEOUT <= SQS_MAX_MESSAGES * (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)`, `OUTBOX_PUBLISH_TIMEOUT + OUTBOX_FINALIZE_TIMEOUT >= OUTBOX_LEASE`, and a `SHUTDOWN_TIMEOUT` no greater than either `SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT` or `OUTBOX_PUBLISH_TIMEOUT`. Both budget calculations reject overflow. Outbox lease validation applies to one singular claim immediately before publication; it is not multiplied by `OUTBOX_BATCH`. Startup also fails when PostgreSQL, SQS, or the queues are unavailable.

## Autenticação

O Keycloak é provisionado automaticamente com clients `client_credentials`:

| Client | Secret | Papel | `provider_id` |
| --- | --- | --- | --- |
| `wallet-service` | `wallet-service-secret` | `wallet-admin` (serviço interno) | — |
| `provider-a` | `provider-a-secret` | `wager-provider` | `provider-a` |
| `provider-b` | `provider-b-secret` | `wager-provider` | `provider-b` |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wager-provider` (token de 3 s, para testes de expiração) | `provider-a` |
| `no-role-client` | `no-role-client-secret` | nenhum | — (sem claim `provider_id`) |

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

Os códigos HTTP, os corpos de erro e os `failureCode` estão em [`ARCHITECTURE.md`](ARCHITECTURE.md#13-contrato-http).

## Testes

```sh
go test ./...          # unitários (sem Docker)
go test -race ./...
make vet               # go vet, também com as tags integration e e2e
make lint              # gofmt/goimports, gocyclo ≤ 6, gocognit ≤ 8 (código e testes)
make                   # vet + lint + testes unitários com -race
```

O CI (`.github/workflows/ci.yml`) roda tidy, gofmt, vet com todas as tags, build, testes unitários com `-race`, `golangci-lint` v2 fixado e `govulncheck` em todo push/PR. A cobertura autoritativa, que precisa de Docker, roda num job à parte disparado manualmente (`workflow_dispatch`) ou toda noite; a política de custo mantém esse job fora de pushes e PRs.

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
make test-e2e           # go test -count=1 -timeout 15m -tags e2e ./test/e2e/...

# Cobertura combinada unidade + integração + e2e, com mínimo de 90,0% por pacote.
make coverage
```

`make coverage` é o gate autoritativo: combina as três suítes (o e2e roda o binário real sem `-race`), instrumenta todos os pacotes com código não-teste em `cmd/`, `internal/` e `test/testenv`, e falha se algum pacote estiver ausente, tiver saída de cobertura inválida ou ficar abaixo de 90,0% de statements. O relatório combinado fica em `coverage/coverage.out` e os percentuais por pacote em `coverage/packages.txt`. Só os testes unitários (`go test ./...`) já cobrem 100% de `app`, `config`, `domain/*`, `observability` e `worker`; o restante dos adaptadores, `bootstrap` e `cli` depende dos containers.

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
