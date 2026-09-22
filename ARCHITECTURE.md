# Arquitetura e decisões

## 1. Organização

```
cmd/wallet                 main: repassa args/env para internal/cli
internal/domain/money      Money (value object)
internal/domain/wallet     Wallet (raiz do agregado) e LedgerEntry
internal/domain/wager      WagerTransaction, máquina de estados, regras (Decide), fingerprint
internal/domain/event      eventos de integração tipados + snapshot para a outbox
internal/app               casos de uso (HTTP, SQS e workers compartilham) e portas
internal/adapter/postgres  pgx + SQL explícito, UnitOfWork, migrations
internal/adapter/httpapi   net/http (ServeMux do Go 1.22+), DTOs, middlewares
internal/adapter/auth      validação OIDC (go-oidc + JWKS)
internal/adapter/sqs       provisionamento, consumidor e publisher
internal/worker            supervisor, relay da outbox, resolvedor de pendências
internal/observability     slog JSON com atributos de contexto, métricas Prometheus
internal/bootstrap         único pacote que conhece o Fx
internal/config, cli       configuração validada e comandos do binário
test/testenv|integration|e2e  containers reais, integração e e2e (build tags)
```

O domínio não importa Fx, HTTP, SQS nem pgx (depende só de `google/uuid` e da biblioteca padrão). `app` define as portas que usa (`UnitOfWork`, repositórios, `Queries`, `OutboxStore`, `Clock`, `IDGenerator`, `Metrics`), e os adaptadores as implementam. Os handlers HTTP dependem de interfaces declaradas no próprio pacote (lado do consumidor).

Complexidade: `golangci-lint` com `gocyclo` ≤ 6 e `gocognit` ≤ 8 vale para todo o código, testes incluídos.

## 2. Dinheiro

- `Money` é imutável: `int64` em unidades mínimas + código ISO 4217. As moedas aceitas (BRL, USD, EUR, GBP, ARS, MXN, CAD) têm todas escala 2.
- Parsing estrito por regex `^(0|[1-9][0-9]*)\.[0-9]{2}$`: rejeita vazio, `NaN`, `Infinity`, notação científica, sinal, escala diferente de 2 e zeros à esquerda. **Não existe normalização**: só a forma canônica é aceita, então o hash de idempotência usa exatamente o texto recebido.
- Limites: `[MinInt64+1, MaxInt64]` unidades mínimas (~±92 quatrilhões de BRL). `MinInt64` é proibido para que `Neg` nunca estoure. Soma, subtração, negação e parsing retornam `ErrOverflow` em vez de dar a volta.
- Aritmética e comparação exigem a mesma moeda (`ErrCurrencyMismatch`). Negativos só existem em cálculos internos (diferença da reconciliação). Entradas externas e saldos são não negativos.
- Nenhum `float` aparece em parsing, cálculo, JSON ou persistência. No JSON, `amount` é sempre string, e um número JSON no lugar é rejeitado com 400. No banco: `BIGINT` + `CHAR(3)`.

## 3. Modelo de domínio

- **Wallet**: `Open` (criação, versão 1; com saldo inicial positivo gera o crédito de abertura sem mudar a versão), `Rehydrate` (sem reaplicar nada) e `Apply(Movement)`, o único caminho que muda o saldo: valida moeda e valor positivo, impede saldo negativo, incrementa a versão e devolve o `LedgerEntry`.
- **LedgerEntry**: o construtor valida `balanceAfter = balanceBefore ± amount`.
- **WagerTransaction**: `NewExternal` (política de zero: `LOSS` = `0.00`, os demais > 0; referência obrigatória em REFUND/ROLLBACK, opcional em WIN, proibida em BET/LOSS), `NewOpening` (sem provedor, chaves, rodada, jogo nem referência) e `Rehydrate`. Transições: `Process`, `Reject`, `Fail` e `AwaitReference`.
- **Regras** (`wager.Decide`, função pura): recebe carteira, operação, referência (ou `nil`) e se a referência já foi revertida, e devolve `Process` (com direção), `Reject(code)` ou `Await`.
- Erros de domínio são sentinelas comparáveis com `errors.Is`. Nenhum `panic` representa regra de negócio.

### Máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PENDING_REFERENCE (nova tentativa) | PROCESSED | REJECTED | FAILED
PROCESSED, REJECTED, FAILED: terminais (o domínio e o trigger do banco recusam transições)
```

- Operações sem dependência são concluídas **de forma síncrona na mesma transação SQL**. `PENDING` existe só em memória e nunca é commitado, então não há aceite assíncrono a retomar. A única espera durável é `PENDING_REFERENCE`, retomada por qualquer instância.
- **Transitório × permanente**: `ErrConflict` (unique violation, versão velha, deadlock, serialização, `lock_timeout`), `ErrUnavailable` (conexão, shutdown do servidor, `statement_timeout`) e cancelamento/timeout de contexto são transitórios e são retentados. Qualquer outro erro é permanente. No worker de pendências, um erro permanente registra `FAILED` / `INTERNAL_FAILURE` para auditoria e interrompe os retries.

## 4. Transações SQL e concorrência

Biblioteca: **pgx v5 com SQL explícito**. `UnitOfWork.Do` abre uma transação `READ COMMITTED` e entrega aos repositórios (carteira, transação, ledger, outbox, inbox) o mesmo `pgx.Tx`. Commit se a função retorna `nil`, rollback caso contrário. Leituras sem lock usam o pool (`Queries`).

Fluxo de uma operação (`WagerService.SubmitInTx`):

1. `SELECT … FROM wallets WHERE id = $1 FOR UPDATE`: **lock pessimista por carteira**. Carteiras diferentes nunca disputam o mesmo lock e não existe lock global.
2. Busca de idempotência (`provider_id` + chave ou `externalTransactionId`). Como roda depois do lock, ela enxerga qualquer operação da carteira já commitada por outra instância.
3. `Decide` + transição em memória.
4. `INSERT` da transação, `UPDATE wallets … WHERE id = $1 AND version = $expected` (**atualização condicionada**: um escritor velho recebe `ErrConflict` e não sobrescreve nada), `INSERT` do ledger, `INSERT` dos eventos na outbox e "despertar" das pendências que referenciam essa operação.
5. Commit.

Corrida na mesma chave via carteiras diferentes: os índices únicos disparam `23505`, que vira `ErrConflict`. A transação é refeita (até `CONFLICT_RETRIES`) e passa a enxergar a vencedora (replay ou conflito). Um `lock_timeout` (5 s) evita espera indefinida atrás de uma carteira travada.

A disputa 80,00 + 80,00 sobre 100,00 se serializa no lock: uma é `PROCESSED` e a outra `REJECTED`/`INSUFFICIENT_FUNDS` (persistida e auditável), saldo final 20,00 e um único débito.

## 5. Invariantes garantidas pelo banco

Independentes de locks locais e da deduplicação do SQS FIFO:

- `wallets.balance_minor >= 0`; `UNIQUE (player_id, currency)`; trigger que proíbe `DELETE`, impede mudança de identidade e exige versão `+1` **exatamente** quando o saldo muda.
- **Constraint triggers adiados** (`DEFERRABLE INITIALLY DEFERRED`), disparados tanto por alterações em `wallets` (`wallets_match_ledger`) quanto por inserções em `ledger_entries` (`ledger_entries_match_wallet`, migration 2): no commit, o saldo guardado precisa ser igual ao `balance_after` do último lançamento. Nenhum saldo muda sem lançamento, e nenhum lançamento entra sem o saldo correspondente.
- `ledger_entries`: check `balance_after = balance_before ± amount`, `UNIQUE (wallet_id, transaction_id)`, trigger de encadeamento (`balance_before` = último `balance_after` e moeda = moeda da carteira), triggers que proíbem `UPDATE`, `DELETE` e `TRUNCATE` (append-only).
- `wager_transactions`: checks de formato interno × externo (OPENING sem provedor/chaves/rodada/jogo/referência), política de zero, referência obrigatória em reversões, `failure_code` em REJECTED/FAILED, resultado em PROCESSED, agenda em PENDING_REFERENCE. Índices únicos: `(provider_id, external_transaction_id)`, `(provider_id, idempotency_key)`, uma `OPENING` por carteira e **uma reversão bem-sucedida por transação referenciada**. Trigger: linhas terminais congeladas e colunas de identidade/payload imutáveis.
- `outbox_events`: snapshot (`payload json`, que preserva o texto exato) e identidade imutáveis, sem `DELETE`, e publicação que não pode ser reescrita.

Todos esses pontos são verificados em `test/integration/schema_test.go`.

## 6. Idempotência

- Persistente em PostgreSQL. Sobrevive ao reinício de todos os processos (testado no e2e com SIGKILL).
- Chave: header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS), **com escopo por provedor** e nunca substituída por uma chave calculada. A mesma chave usada por outro provedor é outra operação, então não há vazamento entre provedores em replays.
- **Hash** (`wager.Fingerprint`): SHA-256 hex do JSON canônico (chaves ordenadas, sem espaços) dos campos de negócio `providerId`, `externalTransactionId`, `playerId`, `walletId` (UUIDs normalizados em minúsculas), `roundId`, `gameId`, `kind`, `money.amount`, `money.currency` e `referenceExternalTransactionId` (omitido se ausente). Ficam fora a chave, os headers, o envelope SQS (`messageId`, `occurredAt`, `type`) e a correlação. HTTP e SQS geram o mesmo hash (testado).
- Regras: mesma chave + mesmo hash → replay do resultado persistido (`idempotentReplay: true`, com o **saldo observado no processamento original**). Mesma chave + hash diferente → `409 IDEMPOTENCY_CONFLICT`. Mesmo `(providerId, externalTransactionId)` com outra chave → `409 DUPLICATE_EXTERNAL_TRANSACTION`.
- Entradas corrigíveis (JSON, Money, UUID, tipo inválido, falta de header) **não são persistidas**: o cliente corrige e reenvia com a mesma chave.

## 7. Operações e reversões

| Tipo | Movimento | Referência |
| --- | --- | --- |
| `BET` | débito; saldo insuficiente → `INSUFFICIENT_FUNDS` | proibida |
| `WIN` | crédito | opcional; se informada, precisa ser uma `BET` do mesmo contexto |
| `LOSS` | nenhum (`0.00`, sem ledger, versão inalterada; emite só `WagerTransactionProcessed`) | proibida |
| `REFUND` | crédito do valor da `BET` | obrigatória, deve ser `BET` |
| `ROLLBACK` | contrário ao original (BET → crédito; WIN/REFUND → débito) | obrigatória: `BET`, `WIN` ou `REFUND` |

- A referência é resolvida por `(providerId, referenceExternalTransactionId)` e precisa concordar em provedor, jogador, carteira, moeda e rodada (`REFERENCE_MISMATCH`). O valor da reversão tem de ser igual ao original (`REVERSAL_AMOUNT_MISMATCH`), sem reversões parciais.
- **REFUND × ROLLBACK sobre a mesma aposta**: cada transação admite **no máximo uma reversão bem-sucedida, de qualquer tipo** (regra de domínio + índice único parcial). Um segundo REFUND ou um ROLLBACK de uma BET já estornada → `ALREADY_REVERSED`. O mesmo débito nunca volta duas vezes. Um `ROLLBACK` de um `REFUND` é permitido uma vez (ele desfaz o estorno, debitando novamente) e deixa a BET com seu estorno já consumido.
- Reversão que precisaria debitar mais que o saldo → `REVERSAL_INSUFFICIENT_FUNDS`, diferente de `INSUFFICIENT_FUNDS`.

## 8. Referências pendentes

- Referência inexistente → `PENDING_REFERENCE` (evento `WagerTransactionPendingReference`, `202`).
- O worker (`PENDING_INTERVAL`) lista as pendências vencidas e, para cada uma, trava a carteira e depois a linha (`FOR UPDATE SKIP LOCKED`, na mesma ordem de locks do fluxo HTTP) e reavalia. Várias instâncias competem sem processar em duplicidade. O estado inteiro vive no banco, então um reinício não perde nada.
- Backoff exponencial `PENDING_BASE_DELAY × 2^tentativas`, limitado a `PENDING_MAX_DELAY`. Quando a referência chega (qualquer operação externa terminal), as pendências que a apontam são "acordadas" na mesma transação, sem esperar o backoff.
- Depois de `PENDING_MAX_ATTEMPTS`: `REJECTED` com `REFERENCE_NOT_FOUND` (nunca chegou) ou `REFERENCE_NOT_PROCESSED` (chegou mas não concluiu), mais o evento de rejeição.
- Referência existente mas ainda pendente → continua esperando. Referência terminada sem sucesso (REJECTED/FAILED) → rejeição imediata `REFERENCE_NOT_PROCESSED`.

## 9. Consumidor SQS e inbox

- Fila `wager-transactions.fifo` com redrive para `wager-transactions-dlq.fifo` (`maxReceiveCount=SQS_MAX_RECEIVE_COUNT`, padrão 5).
- **Vínculo remetente → provedor**: o consumidor lê o atributo de sistema `SenderId` (o ID do usuário ou papel IAM que enviou a mensagem) e só aceita o `providerId` se `SQS_SENDER_PROVIDERS` permitir aquele remetente para aquele provedor. Isso é o equivalente ao `providerId` derivado do token no HTTP. Remetente não autorizado → DLQ, sem efeito financeiro. No LocalStack todo remetente é o account id `000000000000`, por isso o padrão local é `000000000000=*`. Em produção, cada provedor tem seu próprio principal IAM mapeado para o seu `providerId`.
- **Contrato de envio**: `MessageGroupId` = `walletId`, o que dá ordem por carteira e paralelismo entre carteiras. `MessageDeduplicationId` = `messageId` do envelope. A deduplicação FIFO (janela de 5 min) é só uma otimização: a garantia vem da inbox e da idempotência.
- Por mensagem, **numa única transação SQL**: `INSERT` na inbox `(consumer_name, message_id)` com o hash (tipo + chave + hash de negócio), processamento da operação (o mesmo caso de uso do HTTP) e conclusão da inbox. Uma reentrega de mensagem já commitada vira `duplicate` e é removida. O mesmo `messageId` com outro conteúdo é permanente e vai para a DLQ.
- `DeleteMessage` só acontece **depois do commit**. Se o processo morrer entre um e outro, a reentrega é absorvida pela inbox (testado).
- Resultados: sucesso ou rejeição de negócio confirmada → delete. Erro transitório → `ChangeMessageVisibility` com backoff `SQS_RETRY_BASE × 2^(receiveCount−1)` (máx. `SQS_RETRY_MAX`), e depois de `maxReceiveCount` o redrive leva à DLQ. Mensagem inválida (JSON, tipo, campos, float) ou erro permanente (conflito de idempotência, carteira inexistente) → cópia para a DLQ com o atributo `failureReason` e delete.
- Tempos: `SQS_PROCESS_TIMEOUT` (20 s) < `SQS_VISIBILITY_TIMEOUT` (30 s), para que uma mensagem nunca seja processada em dobro enquanto ainda está em andamento. Long polling de `SQS_WAIT_TIME`.
- `SIGTERM`: o polling para (a chamada em curso é cancelada). A mensagem em processamento termina num contexto desacoplado e com prazo. As mensagens do lote que ainda não começaram voltam a ficar visíveis (`visibility = 0`) para reentrega segura.

## 10. Transactional outbox

- Os eventos são gravados na mesma transação do estado, do saldo, do ledger e da inbox. Nada é publicado antes do commit.
- O relay (uma goroutine por instância) faz `UPDATE … WHERE event_id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, que reivindica um lote com **lease** (`locked_by`, `locked_until`). Publishers concorrentes não pegam o mesmo registro enquanto o lease vale. Se um publisher morrer, o lease expira e outra instância assume.
- **Ordem por carteira**: cada rodada reivindica, por `partition_key` (carteira), apenas o evento não publicado mais antigo, e só se ele estiver vencido e sem lease. Um evento posterior nunca sai antes do anterior, mesmo que o anterior esteja em backoff ou nas mãos de outro relay, então o SQS FIFO recebe os eventos de cada carteira na ordem do banco. Para drenar carteiras com vários eventos, cada tick executa rodadas até não haver mais nada vencido (limite de 20). O custo é que uma falha no evento da frente segura os demais daquela carteira até o retry, o que é intencional.
- Falha de publicação → libera o lease e agenda `next_attempt_at` com backoff exponencial (`attempts`, `last_error`). Confirmação: `published_at` apenas se o lease ainda for do dono.
- **Republicação preserva o `eventId`**: ele está no payload imutável e é o `MessageDeduplicationId`. Dentro de 5 min o SQS FIFO descarta a duplicata. Depois disso, o consumidor deduplica por `eventId` (entrega at-least-once).
- Destino: `wallet-events.fifo`, com `MessageGroupId` = `walletId` (ordem por carteira) e atributos `eventType`, `eventId` e `aggregateType`.

### Eventos

Envelope: `eventId`, `eventType`, `aggregateType`, `aggregateId`, `correlationId`, `causationId` (opcional; o `messageId` SQS), `occurredAt` (UTC RFC 3339, ms), `version` (1) e `data` tipado. Valores monetários em `{"amount":"25.00","currency":"BRL"}`.

| Evento | Agregado | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | transação | dados da transação + `balance` + `referenceTransactionId?` |
| `WagerTransactionRejected` | transação | dados da transação + `failureCode` |
| `WagerTransactionPendingReference` | transação | dados da transação + `nextAttemptAt` |
| `WalletBalanceChanged` | carteira | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |

A abertura interna emite `WagerTransactionProcessed` (com `origin: INTERNAL`, sem campos externos) e `WalletBalanceChanged`. Com saldo inicial zero, não emite nenhum evento nem cria `OPENING`.

## 11. Autenticação e autorização

- **IdP: Keycloak** (recomendado pelo desafio, OIDC completo, import declarativo do realm). Serviços usam `client_credentials`. O serviço não emite tokens e não guarda senhas.
- **Validação**: JWT RS256 contra o JWKS (`go-oidc`, com cache e rotação por `kid`), `iss` = `OIDC_ISSUER`, `aud` contém `wallet-api` (audience mapper), `exp` sem tolerância. Emissor e URL do JWKS são configuráveis separadamente, o que permite ao container buscar as chaves por `keycloak:8080` enquanto o `iss` público é `localhost:8180`.
- **Permissões** (papéis de realm em `realm_access.roles`):
  - `wallet-admin` (client `wallet-service`): `POST /wallets`, leitura de carteira, ledger e reconciliação, e leitura de qualquer transação.
  - `wager-provider`: `POST /wagering/transactions` e leitura **das próprias** transações.
- No SQS, o mesmo vínculo é feito pelo `SenderId` da mensagem (§9).
- **A identidade define o `providerId`**: claim `provider_id` (hardcoded mapper por client). Um corpo com outro `providerId` → `403`. `GET /wagering/transactions/{id}` de outro provedor → `404` (não revela existência). `/providers/{outro}/…` → `403`. OPENING só é visível ao serviço interno.
- Um papel ausente, o provedor sem `provider_id` ou um token inválido/expirado não chegam aos casos de uso, então não há efeito financeiro nem exposição de dados (testado com Keycloak real).
- **Broker**: o acesso ao SQS usa credenciais AWS (`AWS_ACCESS_KEY_ID`/`SECRET`). Em produção, a política recomendada é IAM por papel: o serviço com `sqs:ReceiveMessage/DeleteMessage/ChangeMessageVisibility/GetQueueAttributes` na fila de entrada, `SendMessage` na DLQ e na fila de eventos, e os provedores só com `SendMessage` na fila de entrada. O LocalStack community não aplica IAM (ver limitações). As validações de domínio no consumidor valem de qualquer forma.

## 12. Uber Fx e ciclo de vida

- Cada camada é um `fx.Module` (`observability`, `postgres`, `sqs`, `app`, `auth`, `worker`, `http`) com construtores via `fx.Provide`/`fx.Annotate(fx.As(...))` e início via `fx.Invoke`. O grafo é validado com `fx.ValidateApp` num teste unitário.
- **Inicialização**: config validada antes do Fx. O `OnStart` do pool faz `Ping`, as URLs das filas são resolvidas (se uma fila não existir, o start falha) e o servidor faz `Listen` antes de responder pronto. `fx.StartTimeout` limita tudo.
- **Workers**: um `worker.Group` supervisiona relay, resolvedor e N consumidores, cada um com contexto cancelável, e loga `worker started/stopped` (término observável). `Stop` cancela e espera, com prazo.
- **Shutdown** (ordem inversa de registro dos hooks): (1) `http.Server.Shutdown` interrompe novas entradas e conclui as requisições em curso; (2) o grupo de workers para, e o consumidor conclui a mensagem atual ou libera as não iniciadas; (3) o pool do PostgreSQL fecha **depois** de todos que o usam. `SHUTDOWN_TIMEOUT` = `fx.StopTimeout`.

## 13. Contrato HTTP

| Situação | Status | Corpo |
| --- | --- | --- |
| Operação processada agora | `201` | `{transactionId, status:"PROCESSED", balance, idempotentReplay:false}` |
| Replay de operação concluída | `200` | idem, `idempotentReplay:true`, saldo original |
| Aguardando referência | `202` | `{transactionId, status:"PENDING_REFERENCE", nextAttemptAt, idempotentReplay}` |
| Rejeição de negócio (ou replay dela) / falha permanente registrada | `422` | `{transactionId, status:"REJECTED"\|"FAILED", failureCode, balance, idempotentReplay}` |
| Entrada inválida (JSON, Money, UUID, tipo, header ausente) | `400` | `{code:"INVALID_REQUEST", message}` |
| Sem token / token inválido ou expirado | `401` | `{code:"UNAUTHORIZED"}` + `WWW-Authenticate` |
| Papel ou provedor não autorizado | `403` | `{code:"FORBIDDEN"}` |
| Carteira / transação inexistente (ou de outro provedor) | `404` | `WALLET_NOT_FOUND` / `TRANSACTION_NOT_FOUND` |
| Conflito | `409` | `IDEMPOTENCY_CONFLICT`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS` |
| Indisponibilidade transitória (banco, lock timeout, disputa esgotada) | `503` | `{code:"SERVICE_UNAVAILABLE"}` + `Retry-After: 1`; repetir com a mesma chave |
| Erro inesperado | `500` | `{code:"INTERNAL_ERROR"}` (detalhes só no log) |

O saldo observado (`balance` do resultado) é persistido com a própria moeda (`result_currency`). Numa rejeição `CURRENCY_MISMATCH`, o replay devolve o saldo na moeda da carteira, e não na moeda da operação.

`X-Correlation-Id` é aceito (ou gerado) e devolvido. O ledger usa um cursor opaco (`base64url("v1:<seq>")`) sobre uma sequência crescente (`seq`), com ordem estável; `limit` vai de 1 a 200 (padrão 50).

## 14. Códigos de falha (`failureCode`)

Todos são **resultados definitivos**, persistidos em `REJECTED`/`FAILED`: a operação não será reaplicada e o replay devolve o mesmo resultado. Entradas corrigíveis não geram `failureCode`. Viram `400` sem persistência (ver §6).

| Código | Significado |
| --- | --- |
| `INSUFFICIENT_FUNDS` | BET maior que o saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | ROLLBACK que debitaria mais que o saldo |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da carteira |
| `PLAYER_WALLET_MISMATCH` | jogador não é o dono da carteira |
| `BALANCE_LIMIT_EXCEEDED` | crédito estouraria o limite de `int64` |
| `REFERENCE_NOT_FOUND` | referência não chegou dentro do limite de tentativas |
| `REFERENCE_NOT_PROCESSED` | referência rejeitada/falhou, ou nunca concluiu |
| `REFERENCE_MISMATCH` | provedor, jogador, carteira, moeda ou rodada diferentes |
| `REFERENCE_KIND_INVALID` | tipo referenciado não permitido (ex.: REFUND de WIN) |
| `REVERSAL_AMOUNT_MISMATCH` | valor da reversão ≠ valor original |
| `ALREADY_REVERSED` | a referência já tem uma reversão bem-sucedida |
| `INTERNAL_FAILURE` | falha permanente de infraestrutura/dados no worker (status `FAILED`) |

## 15. Reconciliação

`POST /wallets/{id}/reconciliation` lê o saldo guardado e as somas de créditos e débitos do ledger (abertura incluída) **num único comando SQL**, portanto num snapshot consistente. `difference = stored − calculated`. Uma divergência aparece na resposta (`consistent:false`), num log `ERROR` e na métrica `wallet_reconciliation_divergences_total`. O saldo nunca é alterado.

## 16. Observabilidade

- Logs JSON (`slog`) com `service`, `instance` e, conforme disponíveis, `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId` e `clientId` (propagados pelo contexto). Tokens, segredos e payloads financeiros completos não são registrados.
- Métricas em `/metrics`: `wager_transactions_total{source,status}`, `wager_duplicates_total`, `wallet_concurrency_conflicts_total`, `wager_processing_seconds`, `wager_pending_resolutions_total`, `sqs_messages_total{outcome=processed|duplicate|retry|dlq|released}`, `outbox_published_total`, `outbox_publish_failures_total`, `outbox_lag_seconds`, `wallet_reconciliation_divergences_total`, `http_requests_total` e `http_request_duration_seconds`, além das métricas de runtime Go e de processo.
- `GET /health/live` (processo) e `GET /health/ready` (ping no PostgreSQL + `GetQueueAttributes` no SQS).

## 17. Testes

- **Unitários** (`go test ./...`, sem Docker): Money (parsing, escala, limites, overflow, moedas), carteira e ledger, estados, regras dos cinco tipos e política de zero, abertura com metadados e eventos, conflito de payload, casos de uso sobre um store em memória com falhas injetáveis, HTTP, consumidor e publisher SQS com API falsa, relay, config e observabilidade.
- **Integração** (`-tags integration`): PostgreSQL, LocalStack e Keycloak reais via testcontainers (ver README).
- **E2E** (`-tags e2e`): o binário real, 3 processos, SIGKILL e reinício.
- Cobertura combinada (`make coverage`): 100% de `cmd/` e `internal/`. Os testes de integração e e2e compartilham um banco, então usam IDs externos únicos, e os testes de outbox rodam em série porque os relays reivindicam a outbox inteira.

## 18. Limitações, interpretações e trabalho não concluído

- **Aceite assíncrono**: não há. Operações sem dependência são concluídas na transação da requisição, e só `PENDING_REFERENCE` é persistido e retomado. Por isso o cenário "interromper depois de confirmar `PENDING`" não se aplica: não existe `PENDING` commitado.
- **Carteira × provedor**: o desafio não associa carteiras a provedores. Qualquer provedor autenticado pode operar numa carteira cujo `walletId`/`playerId` conheça (as operações e leituras continuam isoladas por provedor).
- **IAM no broker**: o LocalStack community não aplica políticas IAM e reporta todo remetente como `000000000000`, então localmente o vínculo `SenderId` → provedor não distingue provedores. A política recomendada está no §11, e o vínculo está no §9.
- **Deduplicação no consumidor de eventos**: depois da janela de 5 min do SQS FIFO, uma republicação chega de novo com o mesmo `eventId`. Deduplicar por `eventId` é responsabilidade dos consumidores de `wallet-events.fifo`.
- **Retenção**: inbox e outbox publicada crescem indefinidamente. Em produção caberia um job de expurgo por idade (o trigger da outbox bloqueia `DELETE`, que precisaria ser liberado para um papel de manutenção).
- **Mensagem para carteira inexistente via SQS** é tratada como permanente (DLQ), não como pendente.
- **Moedas**: só moedas com escala 2. JPY/KWD etc. exigiriam escala por moeda.
- Opcionais **não implementados**: ledger de partidas dobradas, tracing OpenTelemetry, dashboards e testes de carga.
