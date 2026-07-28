# Modelo de dados DynamoDB do Orchestrator

## Visão geral

A tabela `pitflow-orchestrator` utiliza o padrão **single-table design**. Apenas
`PK` e `SK` são obrigatórias em todos os itens. Os demais atributos são
esparsos e dependem de `entityType`; portanto, a ausência de `messageType` em
um item `SAGA` é esperada.

Valores exibidos pelo console como `S`, `N` e `BOOL` representam tipos nativos
do DynamoDB: string, número e booleano. Eles não fazem parte do nome do campo.

## Tipos de item

| `entityType` | Finalidade | Padrão de chave | Atributos principais |
|---|---|---|---|
| `SAGA` | Estado atual e dados de correlação da SAGA | `PK=SAGA#{sagaId}`, `SK=METADATA` | `sagaId`, `serviceOrderId`, `state`, `amount`, `currency`, `version`, `paymentId`, `preferenceId`, `checkoutUrl`, timestamps |
| `INBOX` | Idempotência de uma mensagem consumida | `PK=MESSAGE#{messageId}`, `SK=INBOX` | `messageType`, `serviceOrderId`, `sagaId` quando conhecido, `processedAt` |
| `OUTBOX` | Mensagem a publicar de forma confiável | `PK=SAGA#{sagaId}`, `SK=OUTBOX#{messageId}` | `messageType`, `destination`, `payload`, `status`, `attempts`, lease/retry e timestamps |
| `HISTORY` | Histórico imutável de transições | `PK=SAGA#{sagaId}`, `SK=EVENT#{occurredAt}#{messageId}` | `eventType`, `messageId`, `state`, `occurredAt`, dados específicos da transição |
| `ACTIVE_SAGA` | Trava de unicidade por ordem de serviço | `PK=ORDER#{serviceOrderId}`, `SK=ACTIVE_SAGA` | `sagaId`, `createdAt` |

## Como funciona a Outbox

A Outbox **não é outra tabela**. Cada mensagem a publicar é apenas mais um item
na tabela `pitflow-orchestrator`, identificado por:

```text
PK = SAGA#{sagaId}
SK = OUTBOX#{messageId}
```

Assim, `SAGA#123 / METADATA` representa o estado atual da SAGA 123, enquanto
`SAGA#123 / OUTBOX#456` representa uma mensagem dessa mesma SAGA que deve ser
enviada ao SQS. Os dois itens compartilham a `PK`, mas têm finalidades e
atributos diferentes.

Exemplo simplificado:

| PK | SK | entityType | messageType | destination | status |
|---|---|---|---|---|---|
| `SAGA#123` | `METADATA` | `SAGA` | — | — | — |
| `SAGA#123` | `OUTBOX#456` | `OUTBOX` | `CreatePayment` | `payment-queue` | `PENDING` |
| `SAGA#123` | `EVENT#2026-07-28T00:00:00Z#789` | `HISTORY` | — | — | — |

O fluxo de publicação é:

1. A transação do DynamoDB atualiza a SAGA e grava o item `OUTBOX` de forma
   atômica. Se a transação falhar, nenhum dos dois é persistido.
2. O item nasce com `status=PENDING`, `attempts=0` e a próxima tentativa em
   `availableAt`.
3. O índice `outbox-by-status` reúne mensagens pendentes de todas as SAGAs. Ele
   evita consultar cada partição individualmente.
4. O publicador reserva a mensagem temporariamente, alterando o estado para
   `PROCESSING` e preenchendo `lockId` e `lockedUntil`.
5. Se o envio ao SQS funcionar, o estado passa para `PUBLISHED`. O item
   continua na tabela para auditoria, mas é removido do índice de pendências.
6. Se o envio falhar, o estado volta para `PENDING`, `attempts` é incrementado
   e `availableAt` recebe uma nova data com backoff.
7. Se um publicador parar enquanto processa uma mensagem, o lease expira e
   outro ciclo pode reservá-la novamente.

Esse mecanismo resolve o intervalo entre persistir o estado da SAGA e publicar
no SQS. A entrega é **pelo menos uma vez**: uma mensagem pode ser reenviada se o
SQS aceitar o envio e a atualização para `PUBLISHED` falhar. Por isso,
consumidores usam `messageId` e Inbox para garantir idempotência.

### Atributos da Outbox

| Atributo | Descrição |
|---|---|
| `messageId` | Identificador único da mensagem e parte da `SK` |
| `messageType` | Comando publicado, por exemplo `CreatePayment` |
| `destination` | Nome da fila SQS de destino |
| `payload` | Envelope JSON completo enviado ao SQS |
| `status` | `PENDING`, `PROCESSING` ou `PUBLISHED` |
| `attempts` | Quantidade de falhas anteriores de publicação |
| `availableAt` | Instante a partir do qual a mensagem pode ser tentada |
| `lockId`, `lockedUntil` | Lease temporário que impede publicação concorrente |
| `lastError` | Nome sanitizado do último erro de publicação |
| `publishedAt` | Instante em que a publicação foi confirmada |
| `GSI3PK`, `GSI3SK` | Chaves do índice `outbox-by-status`; removidas após sucesso |

## Atributos do item `SAGA`

| Atributo | Obrigatório | Descrição |
|---|---:|---|
| `sagaId` | Sim | Identificador da SAGA |
| `serviceOrderId` | Sim | Ordem de serviço correlacionada |
| `state` | Sim | Estado atual da máquina de estados |
| `version` | Sim | Versão usada no controle otimista |
| `amount` | Sim | Valor decimal serializado como string para evitar perda de precisão |
| `currency` | Sim | Moeda ISO, atualmente `BRL` |
| `createdAt` | Sim | Criação da SAGA em ISO-8601/UTC |
| `updatedAt` | Sim | Última mudança em ISO-8601/UTC |
| `paymentId` | Após criação do pagamento | Identificador local do Payment |
| `preferenceId` | Após criação do link | Preferência do Mercado Pago |
| `checkoutUrl` | Após criação do link | URL hospedada do Checkout Pro |
| `paymentExpiresAt` | Após criação do link | Expiração funcional da preferência |
| `operationConfirmedAt` | Após confirmação | Momento em que Operation confirmou `AWAITING_PAYMENT` |

`paymentExpiresAt` não é o TTL da tabela. O TTL do DynamoDB usa o atributo
`expiresAt`, em epoch seconds, quando um item for elegível para expurgo.

## Índices

| Índice | Chaves | Uso |
|---|---|---|
| Primário | `PK`, `SK` | Consultar agregado, histórico e outbox por SAGA; inbox por mensagem |
| `by-service-order` | `GSI1PK`, `GSI1SK` | Encontrar a SAGA por OS (`ORDER#{serviceOrderId}`) |
| `by-status-timeout` | `GSI2PK`, `GSI2SK` | Reservado para busca de SAGAs por estado/timeout |
| `outbox-by-status` | `GSI3PK`, `GSI3SK` | Buscar outbox pendente ou com lease expirado |

Os GSIs são esparsos: um item só aparece no índice quando possui as duas chaves
daquele índice.
