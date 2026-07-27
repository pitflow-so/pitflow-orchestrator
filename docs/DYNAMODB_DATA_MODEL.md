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
