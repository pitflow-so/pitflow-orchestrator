# PitFlow Orchestrator

Microsserviço responsável por coordenar a SAGA de pagamento das ordens de
serviço. Ele mantém o estado e o histórico da SAGA no DynamoDB, consome eventos
de negócio, publica comandos para Operation e Payment e decide quando o fluxo
termina em `COMPLETED` ou `FAILED`.

## Por que a SAGA é orquestrada

O fluxo atravessa Operation, Payment, Mercado Pago, PostgreSQL, DynamoDB e SQS.
Não existe uma transação ACID única capaz de confirmar ou desfazer todos esses
recursos.

Foi escolhida uma **SAGA orquestrada** porque:

- o estado e as transições do processo ficam explícitos em uma máquina de
  estados central;
- o caminho feliz e as compensações são auditáveis por `sagaId`,
  `serviceOrderId`, histórico e traces;
- regras de timeout, retry, evento fora de ordem e estado terminal ficam em um
  único proprietário;
- Operation e Payment permanecem responsáveis apenas por suas transações
  locais;
- o fluxo é mais simples de demonstrar e operar do que uma coreografia em que
  cada serviço precisa conhecer implicitamente a próxima etapa.

O Orchestrator não acessa bancos de outros serviços. Ele coordena por mensagens
e persiste somente seus próprios dados no DynamoDB.

## Por que a comunicação usa Amazon SQS

Foi escolhida a **Amazon SQS Standard** para comandos e eventos assíncronos
porque ela:

- desacopla a disponibilidade dos serviços;
- oferece serviço gerenciado compatível com o ambiente AWS do projeto;
- permite retry, long polling e DLQ sem operar um broker próprio;
- suporta a escala independente dos consumidores;
- integra-se diretamente à infraestrutura, IAM e observabilidade já adotadas.

SQS Standard possui entrega **pelo menos uma vez** e pode entregar mensagens
duplicadas ou fora de ordem. Por isso, a solução não depende de entrega
exatamente uma vez:

- cada envelope possui `messageId`, `correlationId`, `sagaId`,
  `serviceOrderId`, `schemaVersion`, `type` e `occurredAt`;
- consumidores registram inbox/idempotência por `messageId`;
- publishers utilizam transactional outbox;
- transições usam condições de estado e versionamento otimista;
- mensagens incompatíveis com o estado atual não avançam a SAGA;
- falhas transitórias são reenviadas e falhas persistentes seguem para DLQ
  após `maxReceiveCount=5`.

### Filas

| Fila | Direção principal | Exemplos |
|---|---|---|
| `service-order-orchestrator-queue` | Serviços → Orchestrator | `ServiceOrderBudgetApproved`, `PaymentLinkCreated`, `PaymentApproved`, `PaymentRejected`, confirmações do Operation |
| `payment-command-queue` | Orchestrator → Payment | `CreatePayment` |
| `operation-command-queue` | Orchestrator → Operation | `MarkServiceOrderAwaitingPayment`, `MarkServiceOrderReadyForExecution`, `CancelServiceOrder` |

Cada fila principal possui sua própria DLQ.

## Fluxo principal

```text
ServiceOrderBudgetApproved
  -> CreatePayment
  -> PaymentLinkCreated
  -> MarkServiceOrderAwaitingPayment
  -> ServiceOrderAwaitingPayment
  -> PaymentApproved
  -> MarkServiceOrderReadyForExecution
  -> ServiceOrderReadyForExecution
  -> SAGA COMPLETED
```

O recebimento de `PaymentApproved` não conclui a SAGA. O estado só chega a
`COMPLETED` depois que o Operation confirma
`ServiceOrderReadyForExecution`.

## Compensação homologada

```text
PaymentRejected
  -> SAGA COMPENSATING
  -> CancelServiceOrder
  -> Operation CANCELLED
  -> ServiceOrderCancelled
  -> SAGA FAILED
```

A compensação desfaz semanticamente o processo distribuído; ela não executa
rollback físico em bancos alheios. Cada serviço confirma sua própria transação
local e publica o resultado para que o Orchestrator avance a máquina de estados.

Esse cenário está coberto pelo BDD E2E do `pitflow-payment`, que comprovou
Payment `REJECTED`, Operation `CANCELLED`, SAGA `FAILED` e replay idempotente.

## Persistência confiável

O Orchestrator usa DynamoDB single-table:

- `SAGA`: estado e correlação;
- `HISTORY`: transições imutáveis;
- `INBOX`: deduplicação;
- `OUTBOX`: publicação confiável com claim, lease e retry;
- `ACTIVE_SAGA`: unicidade de SAGA ativa por ordem.

Atualizações relacionadas são realizadas com escritas condicionais e
`TransactWriteItems`. Consulte [o modelo DynamoDB](docs/DYNAMODB_DATA_MODEL.md).

Operation e Payment usam PostgreSQL com transactional outbox. Assim, a mudança
de negócio e o evento são persistidos na mesma transação local; um publisher
posterior entrega o evento ao SQS.

## Alternativas consideradas

- **SAGA coreografada:** descartada porque distribuiria as regras de sequência
  e compensação entre serviços, dificultando auditoria, testes e operação.
- **Chamadas HTTP síncronas para todo o fluxo:** descartadas por acoplar a
  disponibilidade dos serviços e aumentar o risco de estado parcial.
- **SQS FIFO:** não foi necessária para o MVP; FIFO não elimina a necessidade
  de idempotência, e a máquina de estados já rejeita duplicatas e eventos fora
  de ordem.
- **Broker autogerenciado:** descartado pelo custo operacional desnecessário no
  AWS Learner Lab.

## Contratos e decisão arquitetural

- [ADR canônico — SAGA orquestrada e mensageria do pagamento](https://github.com/pitflow-so/pitflow-bootstrap/blob/main/docs/adr/ADR-001-saga-messaging.md)
- [Contrato AsyncAPI da SAGA](https://github.com/pitflow-so/pitflow-bootstrap/blob/main/contracts/asyncapi/pitflow-saga-v1.yaml)
- [Modelo de dados DynamoDB](docs/DYNAMODB_DATA_MODEL.md)

## Execução e entrega

```bash
mvn -B clean verify
```

O repositório possui Dockerfile, manifests Kubernetes e pipeline independente
de build, testes, publicação da imagem e deploy no EKS. Configurações e
credenciais são recebidas por variáveis de ambiente e AWS Secrets Manager; não
devem ser versionadas.

## Limites

O endpoint acadêmico usado para provocar rejeição pertence ao Payment e deve ser
removido ou condicionado por perfil antes de uso comercial. Evoluções como
expiração, estorno, reconciliação e notificação dedicada não fazem parte do
recorte mínimo homologado.
