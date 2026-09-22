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
internal/contract          formas de wire compartilhadas por HTTP e SQS (Money, Operation, decoder estrito)
internal/adapter/httpapi   net/http (ServeMux do Go 1.22+), DTOs, middlewares
internal/adapter/auth      validação OIDC (JWKS com cache próprio, RS256)
internal/adapter/sqs       provisionamento, consumidor e publisher
internal/worker            supervisor, relay da outbox, resolvedor de pendências, contextos desacoplados do shutdown
internal/observability     slog JSON com atributos de contexto, métricas Prometheus
internal/bootstrap         único pacote que conhece o Fx
internal/config, cli       configuração validada e comandos do binário
test/testenv|integration|e2e  containers reais, integração e e2e (build tags)
```

O domínio não importa Fx, HTTP, SQS nem pgx (depende só de `google/uuid` e da biblioteca padrão). `app` define as portas que usa (`UnitOfWork`, repositórios, `Queries`, `OutboxStore`, `Clock`, `IDGenerator`, `Metrics`), e os adaptadores as implementam. Os handlers HTTP dependem de interfaces declaradas no próprio pacote (lado do consumidor). `contract` guarda o que HTTP e SQS têm em comum (o objeto `{amount, currency}`, o corpo da operação e o decoder JSON estrito: um único objeto, sem campos desconhecidos, nada depois dele), para que as duas entradas rejeitem exatamente as mesmas coisas.

Um único helper de backoff exponencial (`app.Backoff(base, máximo, expoente)` = `base × 2^expoente`, limitado ao máximo e sem overflow) serve às referências pendentes, ao consumidor SQS e ao relay da outbox.

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
- **WagerTransaction** (a reidratação também valida a referência, então uma linha corrompida vira erro, nunca pânico nas regras): `NewExternal` (política de zero: `LOSS` = `0.00`, os demais > 0; referência obrigatória em REFUND/ROLLBACK, opcional em WIN, proibida em BET/LOSS), `NewOpening` (sem provedor, chaves, rodada, jogo nem referência) e `Rehydrate`. Transições: `Process`, `Reject`, `Fail` e `AwaitReference`.
- **Regras** (`wager.Decide`, função pura): recebe carteira, operação, referência (ou `nil`) e se a referência já foi revertida, e devolve `Process` (com direção), `Reject(code)` ou `Await`. A direção de um `ROLLBACK` vem de uma tabela explícita (BET → crédito; WIN/REFUND → débito); qualquer alvo fora dela é rejeitado com `REFERENCE_KIND_INVALID`, nunca "processado sem mover dinheiro".
- **Validações do construtor** que o banco não cobre: uma operação não pode referenciar a si mesma; `Process` exige referência resolvida em REFUND/ROLLBACK e a proíbe em BET/LOSS; `Reject` e `Fail` exigem `failureCode`, e `Reject` valida o saldo observado.
- Erros de domínio são sentinelas comparáveis com `errors.Is`. Nenhum `panic` representa regra de negócio.

### Máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PENDING_REFERENCE (nova tentativa) | PROCESSED | REJECTED | FAILED
PROCESSED, REJECTED, FAILED: terminais (o domínio e o trigger do banco recusam transições)
```

- Operações sem dependência são concluídas **de forma síncrona na mesma transação SQL**. `PENDING` existe só em memória e nunca é commitado, então não há aceite assíncrono a retomar. A única espera durável é `PENDING_REFERENCE`, retomada por qualquer instância.
- **Transitório × permanente**: `ErrConflict` (unique violation, versão velha, deadlock, serialização, `lock_timeout`), `ErrUnavailable` (classes SQLSTATE 08, 53 e 58, `40003`, `statement_timeout`, pool fechado, shutdown do servidor) e cancelamento/timeout de contexto (repassados sem reclassificar) são transitórios e são retentados. Qualquer outro erro é permanente. No worker de pendências, um erro permanente registra `FAILED` / `INTERNAL_FAILURE`, emite `WagerTransactionFailed` e acorda as pendências que referenciam a operação, exatamente como uma rejeição.

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
- `wager_transactions`: referência externa nunca vazia (migration 3); checks de formato interno × externo (OPENING sem provedor/chaves/rodada/jogo/referência), política de zero, referência obrigatória em reversões, `failure_code` em REJECTED/FAILED, resultado em PROCESSED, agenda em PENDING_REFERENCE. Índices únicos: `(provider_id, external_transaction_id)`, `(provider_id, idempotency_key)`, uma `OPENING` por carteira e **uma reversão bem-sucedida por transação referenciada**. Trigger: linhas terminais congeladas e colunas de identidade/payload imutáveis.
- `outbox_events`: snapshot (`payload json`, que preserva o texto exato) e identidade imutáveis, sem `DELETE`, e publicação que não pode ser reescrita.

- Migration 4: o `CHECK` de `status` deixa de admitir `PENDING` (que só existe em memória) e a outbox ganha `outbox_events_single_outcome` (publicado ou dead-lettered, nunca os dois) e `outbox_events_lease_pair` (`locked_by` e `locked_until` sempre juntos).

Todos esses pontos são verificados em `test/integration/schema_test.go`. O pool (`pgx`) usa `MinConns=1`, `MaxConnLifetime=1h`, `MaxConnIdleTime=30m` e `HealthCheckPeriod=1m`, além de `lock_timeout`/`statement_timeout` por conexão. `Transactions.Save` de uma linha inexistente devolve `ErrTransactionNotFound` e `Inbox.Complete` de uma mensagem não registrada é um erro permanente, para que um bug de fluxo nunca passe em silêncio.

## 6. Idempotência

- Persistente em PostgreSQL. Sobrevive ao reinício de todos os processos (testado no e2e com SIGKILL).
- Chave: header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS), **com escopo por provedor** e nunca substituída por uma chave calculada. A mesma chave usada por outro provedor é outra operação, então não há vazamento entre provedores em replays.
- **Hash** (`wager.Fingerprint`): SHA-256 hex do JSON canônico (chaves ordenadas, sem espaços, sem escape HTML de `<`, `>` e `&`) dos campos de negócio `providerId`, `externalTransactionId`, `playerId`, `walletId` (UUIDs normalizados em minúsculas), `roundId`, `gameId`, `kind`, `money.amount`, `money.currency` e `referenceExternalTransactionId` (omitido se ausente). Ficam fora a chave, os headers, o envelope SQS (`messageId`, `occurredAt`, `type`) e a correlação. HTTP e SQS geram o mesmo hash (testado).
- Regras: mesma chave + mesmo hash → replay do resultado persistido (`idempotentReplay: true`, com o **saldo observado no processamento original**). Mesma chave + hash diferente → `409 IDEMPOTENCY_CONFLICT`. Mesmo `(providerId, externalTransactionId)` com outra chave → `409 DUPLICATE_EXTERNAL_TRANSACTION`.
- Entradas corrigíveis (JSON, Money, UUID, tipo inválido, falta de header) **não são persistidas**: o cliente corrige e reenvia com a mesma chave.

## 7. Operações e reversões

| Tipo | Movimento | Referência |
| --- | --- | --- |
| `BET` | débito; saldo insuficiente → `INSUFFICIENT_FUNDS` | proibida |
| `WIN` | crédito | opcional; se informada, precisa ser uma `BET` do mesmo contexto (se ela ainda não chegou, a WIN espera como uma reversão e expira com `REFERENCE_NOT_FOUND`) |
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
- Depois de `PENDING_MAX_ATTEMPTS`: `REJECTED` com `REFERENCE_NOT_FOUND` (nunca chegou) ou `REFERENCE_NOT_PROCESSED` (chegou mas não concluiu), mais o evento de rejeição. As resoluções do worker contam em `wager_transactions_total{source="worker"}` e `wager_processing_seconds{source="worker"}`.
- Referência existente mas ainda pendente → continua esperando. Referência terminada sem sucesso (REJECTED/FAILED) → rejeição imediata `REFERENCE_NOT_PROCESSED`.

## 9. Consumidor SQS e inbox

- Fila `wager-transactions.fifo` com redrive para `wager-transactions-dlq.fifo` (`maxReceiveCount=SQS_MAX_RECEIVE_COUNT`, padrão 5).
- **Vínculo remetente → provedor**: o consumidor lê o atributo de sistema `SenderId` (o ID do usuário ou papel IAM que enviou a mensagem) e só aceita o `providerId` se `SQS_SENDER_PROVIDERS` permitir aquele remetente para aquele provedor. Isso é o equivalente ao `providerId` derivado do token no HTTP. Remetente não autorizado → DLQ, sem efeito financeiro. No LocalStack todo remetente é o account id `000000000000`, por isso o padrão local é `000000000000=*`. Em produção, cada provedor tem seu próprio principal IAM mapeado para o seu `providerId`.
- **Contrato de envio**: `MessageGroupId` = `walletId`, o que dá ordem por carteira e paralelismo entre carteiras. `MessageDeduplicationId` = `messageId` do envelope. A deduplicação FIFO (janela de 5 min) é só uma otimização: a garantia vem da inbox e da idempotência.
- Por mensagem, **numa única transação SQL**: `INSERT` na inbox `(consumer_name, message_id)` com o hash (tipo + chave + hash de negócio), processamento da operação (o mesmo caso de uso do HTTP) e conclusão da inbox. Uma reentrega de mensagem já commitada vira `duplicate` e é removida. O mesmo `messageId` com outro conteúdo é permanente e vai para a DLQ.
- **Ordem do grupo dentro de um lote**: um `ReceiveMessage` pode trazer várias mensagens do mesmo `MessageGroupId` (a cauda em ordem de uma carteira). Se a primeira for para retry, as seguintes do mesmo grupo não são processadas naquele lote: elas são liberadas (visibilidade 0) e voltam em ordem depois da cabeça.
- `DeleteMessage` só acontece **depois do commit**. Se o processo morrer entre um e outro, a reentrega é absorvida pela inbox (testado).
- Resultados: sucesso ou rejeição de negócio confirmada → delete. Erro transitório → `ChangeMessageVisibility` com backoff `SQS_RETRY_BASE × 2^(receiveCount−1)` (máx. `SQS_RETRY_MAX`), e depois de `maxReceiveCount` o redrive leva à DLQ. Mensagem inválida (JSON, tipo, campos, float) ou erro permanente (conflito de idempotência, carteira inexistente) → cópia para a DLQ com o atributo `failureReason` (truncado sem quebrar UTF-8) e o `MessageGroupId` original (a ordem por carteira sobrevive a um replay da DLQ), e delete. Se a cópia para a DLQ falhar, a mensagem fica e o resto do seu grupo é liberado, para não passar à frente dela.
- Timing: every message returned by one `ReceiveMessage` starts the same visibility window and the consumer processes that batch serially. Configuration therefore enforces, with overflow-safe arithmetic, `SQS_VISIBILITY_TIMEOUT > SQS_MAX_MESSAGES * (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)`; the defaults satisfy `5m > 10 * (20s + 5s)`. `SQS_PROCESS_TIMEOUT` bounds each business operation and `SQS_ACK_TIMEOUT` separately bounds its broker follow-up (delete, visibility change, or DLQ copy). A larger batch amortizes receive calls but requires a longer visibility window, which delays redelivery after a consumer crash; shortening visibility below the full-batch budget permits concurrent duplicate processing. `SQS_WAIT_TIME` controls long polling.
- `SIGTERM`: o polling para (a chamada em curso é cancelada). A mensagem em processamento termina num contexto desacoplado e com prazo. As mensagens do lote que ainda não começaram voltam a ficar visíveis (`visibility = 0`) para reentrega segura.

## 10. Transactional outbox

- Os eventos são gravados na mesma transação do estado, do saldo, do ledger e da inbox. Nada é publicado antes do commit.
- O relay (uma goroutine por instância) faz `UPDATE … WHERE event_id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, que reivindica um lote com **lease** (`locked_by`, `locked_until`). Publishers concorrentes não pegam o mesmo registro enquanto o lease vale. Se um publisher morrer, o lease expira e outra instância assume.
- **Ordem por carteira**: cada rodada reivindica, por `partition_key` (carteira), apenas o evento não publicado mais antigo, e só se ele estiver vencido e sem lease. Um evento posterior nunca sai antes do anterior, mesmo que o anterior esteja em backoff ou nas mãos de outro relay, então o SQS FIFO recebe os eventos de cada carteira na ordem do banco. Para drenar carteiras com vários eventos, cada tick executa rodadas até não haver mais nada vencido (limite de 20). O custo é que uma falha no evento da frente segura os demais daquela carteira até o retry, o que é intencional.
- Cada publicação tem `OUTBOX_PUBLISH_TIMEOUT` (10 s, obrigatoriamente menor que `OUTBOX_LEASE`, para que um envio lento nunca sobreviva ao próprio lease) e roda num contexto desacoplado do `SIGTERM`; entre uma publicação e outra o relay verifica o cancelamento e devolve o restante do lote ao expirar do lease.
- Falha de publicação → libera o lease e agenda `next_attempt_at` com backoff exponencial (`attempts`, `last_error`). Depois de `OUTBOX_MAX_ATTEMPTS` falhas, o evento recebe `dead_lettered_at` (fica guardado para auditoria e replay manual; métrica `outbox_dead_lettered_total` e log `ERROR`) e deixa de bloquear os eventos seguintes da carteira. Confirmação: `published_at` apenas se o lease ainda for do dono.
- **Republicação preserva o `eventId`**: ele está no payload imutável e é o `MessageDeduplicationId`. Dentro de 5 min o SQS FIFO descarta a duplicata. Depois disso, o consumidor deduplica por `eventId` (entrega at-least-once).
- Destino: `wallet-events.fifo`, com `MessageGroupId` = `walletId` (ordem por carteira) e atributos `eventType`, `eventId` e `aggregateType`.

### Eventos

Envelope: `eventId`, `eventType`, `aggregateType`, `aggregateId`, `correlationId`, `causationId` (opcional; o `messageId` SQS), `occurredAt` (UTC RFC 3339, ms), `version` (1) e `data` tipado. Valores monetários em `{"amount":"25.00","currency":"BRL"}`.

| Evento | Agregado | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | transação | dados da transação + `balance` + `referenceTransactionId?` |
| `WagerTransactionRejected` | transação | dados da transação + `failureCode` |
| `WagerTransactionFailed` | transação | dados da transação + `failureCode` (`INTERNAL_FAILURE`, emitido pelo worker de pendências) |
| `WagerTransactionPendingReference` | transação | dados da transação + `nextAttemptAt` |
| `WalletBalanceChanged` | carteira | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |

A abertura interna emite `WagerTransactionProcessed` (com `origin: INTERNAL`, sem campos externos) e `WalletBalanceChanged`. Com saldo inicial zero, não emite nenhum evento nem cria `OPENING`.

## 11. Autenticação e autorização

- **IdP: Keycloak** (recomendado pelo desafio, OIDC completo, import declarativo do realm). Serviços usam `client_credentials`. O serviço não emite tokens e não guarda senhas.
- **Validação**: JWT RS256 contra o JWKS (`go-oidc` para as claims; o cache de chaves é próprio, com rotação por `kid`), `iss` = `OIDC_ISSUER`, `aud` contém `wallet-api` (audience mapper), `exp` sem tolerância (`nbf` tem os 5 min de tolerância do `go-oidc`). Emissor e URL do JWKS são configuráveis separadamente, o que permite ao container buscar as chaves por `keycloak:8080` enquanto o `iss` público é `localhost:8180`. Cada busca do JWKS tem timeout de 5 s, e um token cujo `kid` não está em cache só dispara uma nova busca a cada 30 s: fora dessa janela ele é recusado com `401`, o que impede que tokens forjados transformem cada requisição numa chamada ao IdP. Tokens rejeitados são registrados em `DEBUG` (o motivo, nunca o token).
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
- **Shutdown** (ordem inversa de registro dos hooks): (1) `http.Server.Shutdown` interrompe novas entradas e conclui as requisições em curso; (2) o grupo de workers para, e o consumidor conclui a mensagem atual ou libera as não iniciadas; (3) o pool do PostgreSQL fecha **depois** de todos que o usam. O hook de parada do grupo é registrado em `startWorkers`, depois de todas as dependências (inclusive o pool) terem registrado os seus, o que garante essa ordem; `TestFxStopsServerThenWorkersThenPool` verifica a sequência real dos hooks. `SHUTDOWN_TIMEOUT` = `fx.StopTimeout` e precisa ser maior que `SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT` e que `OUTBOX_PUBLISH_TIMEOUT`, para que o trabalho em andamento caiba no encerramento. Um segundo `SIGTERM`/`SIGINT` durante o encerramento devolve o tratamento padrão do Go (encerra na hora).
- Os eventos internos do Fx são logados em `DEBUG`.

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
| Rota inexistente / método não permitido | `404` / `405` | `NOT_FOUND` / `METHOD_NOT_ALLOWED` (com `Allow`) |
| Corpo maior que 64 KiB | `413` | `PAYLOAD_TOO_LARGE` |
| `Content-Type` presente e diferente de `application/json` | `415` | `UNSUPPORTED_MEDIA_TYPE` |
| Conflito | `409` | `IDEMPOTENCY_CONFLICT`, `DUPLICATE_EXTERNAL_TRANSACTION`, `WALLET_ALREADY_EXISTS` |
| Indisponibilidade transitória (banco, lock timeout, disputa esgotada) | `503` | `{code:"SERVICE_UNAVAILABLE"}` + `Retry-After: 1`; repetir com a mesma chave |
| Erro inesperado | `500` | `{code:"INTERNAL_ERROR"}` (detalhes só no log) |

As mensagens de erro são fixas por código, então o contexto interno de erros encapsulados nunca chega ao cliente. A exceção é `INVALID_REQUEST`, cuja mensagem descreve a própria entrada do cliente para que ele a corrija, em termos do contrato e nunca de tipos internos: `body is required`, `malformed JSON at offset N`, `field initialBalance.amount must be a string`, `unknown field "x"`, `body must contain a single JSON object`. Toda resposta, inclusive 404/405, é JSON, passa pelo log de acesso e pelas métricas (rota `unmatched`) e devolve `X-Correlation-Id`. O esquema `Bearer` do header `Authorization` é aceito sem diferenciar maiúsculas (RFC 6750). O servidor aplica `ReadHeaderTimeout` 5 s, `ReadTimeout` 15 s e `WriteTimeout` 30 s; um `panic` num handler vira `500` só se nada foi escrito ainda (e `http.ErrAbortHandler` é repassado).

`Idempotency-Key` é obrigatório, tem espaços das pontas removidos, no máximo 128 caracteres e só ASCII imprimível. UUIDs de rota não podem ser nulos.

O saldo observado (`balance` do resultado) é persistido com a própria moeda (`result_currency`). Numa rejeição `CURRENCY_MISMATCH`, o replay devolve o saldo na moeda da carteira, e não na moeda da operação.

`X-Correlation-Id` é aceito se casar com `^[A-Za-z0-9._:-]{1,128}$` (senão é substituído por um UUID gerado) e devolvido. O ledger usa um cursor opaco (`base64url("v1:<seq>")`) sobre uma sequência crescente (`seq`), com ordem estável; `limit`, quando informado, vai de 1 a 200 (`limit=0` é `400`); ausente, vale 50.

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
- Métricas em `/metrics`: `wager_transactions_total{source,status}`, `wager_duplicates_total`, `wallet_concurrency_conflicts_total`, `wager_processing_seconds`, `wager_pending_resolutions_total`, `sqs_messages_total{outcome=processed|duplicate|retry|dlq|released}`, `outbox_published_total`, `outbox_publish_failures_total`, `outbox_dead_lettered_total`, `outbox_lag_seconds`, `wallet_reconciliation_divergences_total`, `http_requests_total` e `http_request_duration_seconds`, além das métricas de runtime Go e de processo.
- `GET /health/live` (processo) e `GET /health/ready` (ping no PostgreSQL + `GetQueueAttributes` no SQS, em paralelo, cada um com `READY_TIMEOUT` inteiro).

## 17. Testes

- **Unitários** (`go test ./...`, sem Docker): Money (parsing, escala, limites, overflow, moedas), carteira e ledger, estados, regras dos cinco tipos e política de zero, abertura com metadados e eventos, conflito de payload, casos de uso sobre um store em memória com falhas injetáveis, HTTP, consumidor e publisher SQS com API falsa, relay, config e observabilidade.
- **Integração** (`-tags integration`): PostgreSQL, LocalStack e Keycloak reais via testcontainers (ver README).
- **E2E** (`-tags e2e`): o binário real, 3 processos, SIGKILL e reinício.
- Helpers compartilhados: `internal/testutil` (sem build tag: dinheiro, relógio falso, registro de métricas, buffer sincronizado) para os unitários, e `test/testenv` (containers, cliente HTTP, corpos de operação, envelope SQS via `sqs.EncodeMessage`) para integração e e2e.
- Cobertura combinada (`make coverage`) de `cmd/` e `internal/`. Os testes de integração e e2e compartilham um banco, então usam IDs externos únicos, e os testes de outbox rodam em série porque os relays reivindicam a outbox inteira. O e2e roda o binário real sem `-race`.

## 18. Limitações, interpretações e trabalho não concluído

- **Aceite assíncrono**: não há. Operações sem dependência são concluídas na transação da requisição, e só `PENDING_REFERENCE` é persistido e retomado. Por isso o cenário "interromper depois de confirmar `PENDING`" não se aplica: não existe `PENDING` commitado.
- **Carteira × provedor**: o desafio não associa carteiras a provedores. Qualquer provedor autenticado pode operar numa carteira cujo `walletId`/`playerId` conheça (as operações e leituras continuam isoladas por provedor).
- **IAM no broker**: o LocalStack community não aplica políticas IAM e reporta todo remetente como `000000000000`, então localmente o vínculo `SenderId` → provedor não distingue provedores. A política recomendada está no §11, e o vínculo está no §9.
- **Deduplicação no consumidor de eventos**: depois da janela de 5 min do SQS FIFO, uma republicação chega de novo com o mesmo `eventId`. Deduplicar por `eventId` é responsabilidade dos consumidores de `wallet-events.fifo`.
- **Provisionamento**: as filas são criadas só com o atributo imutável `FifoQueue`, e os atributos mutáveis (redrive, visibilidade) são aplicados com `SetQueueAttributes`. Por isso reexecutar `provision-queues` reconcilia filas existentes. Trocar uma fila FIFO por uma padrão (ou o contrário) exige recriá-la.
- **Retenção**: inbox e outbox publicada crescem indefinidamente. Em produção caberia um job de expurgo por idade (o trigger da outbox bloqueia `DELETE`, que precisaria ser liberado para um papel de manutenção).
- **Mensagem para carteira inexistente via SQS** é tratada como permanente (DLQ), não como pendente.
- **Moedas**: só moedas com escala 2. JPY/KWD etc. exigiriam escala por moeda.
- Opcionais **não implementados**: ledger de partidas dobradas, tracing OpenTelemetry, dashboards e testes de carga.
