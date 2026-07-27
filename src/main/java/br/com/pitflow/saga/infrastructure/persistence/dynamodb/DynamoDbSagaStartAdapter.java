package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.PaymentSaga;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.CreatePaymentCommand;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class DynamoDbSagaStartAdapter implements SagaStartGateway {
    private static final String ITEM_ABSENT =
            "attribute_not_exists(PK) AND attribute_not_exists(SK)";

    private final DynamoDbClient client;
    private final ObjectMapper objectMapper;
    private final String tableName;
    private final String paymentDestination;

    public DynamoDbSagaStartAdapter(
            DynamoDbClient client,
            ObjectMapper objectMapper,
            String tableName,
            String paymentDestination
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.tableName = tableName;
        this.paymentDestination = paymentDestination;
    }

    @Override
    public StartResult startAtomically(
            BudgetApprovedEvent source,
            PaymentSaga saga,
            CreatePaymentCommand command
    ) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            put(inbox(source)),
                            put(activeSagaGuard(saga)),
                            put(sagaMetadata(saga)),
                            put(history(source, saga)),
                            put(outbox(command))
                    )
                    .build());
            return StartResult.CREATED;
        } catch (TransactionCanceledException exception) {
            if (alreadyExists(inboxKey(source))
                    || alreadyExists(activeSagaKey(saga))) {
                return StartResult.ALREADY_PROCESSED;
            }
            throw exception;
        }
    }

    private TransactWriteItem put(Map<String, AttributeValue> item) {
        return TransactWriteItem.builder()
                .put(Put.builder()
                        .tableName(tableName)
                        .item(item)
                        .conditionExpression(ITEM_ABSENT)
                        .build())
                .build();
    }

    private boolean alreadyExists(Map<String, AttributeValue> key) {
        return client.getItem(GetItemRequest.builder()
                        .tableName(tableName)
                        .key(key)
                        .consistentRead(true)
                        .projectionExpression("PK")
                        .build())
                .hasItem();
    }

    private Map<String, AttributeValue> inbox(BudgetApprovedEvent event) {
        var item = new LinkedHashMap<>(inboxKey(event));
        item.put("entityType", s("INBOX"));
        item.put("messageType", s("ServiceOrderBudgetApproved"));
        item.put("serviceOrderId", s(event.serviceOrderId().toString()));
        item.put("processedAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> activeSagaGuard(PaymentSaga saga) {
        var item = new LinkedHashMap<>(activeSagaKey(saga));
        item.put("entityType", s("ACTIVE_SAGA"));
        item.put("sagaId", s(saga.sagaId().toString()));
        item.put("createdAt", s(saga.createdAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> sagaMetadata(PaymentSaga saga) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + saga.sagaId()));
        item.put("SK", s("METADATA"));
        item.put("entityType", s("SAGA"));
        item.put("sagaId", s(saga.sagaId().toString()));
        item.put("serviceOrderId", s(saga.serviceOrderId().toString()));
        item.put("state", s(saga.state().name()));
        item.put("amount", s(saga.amount().serializedAmount()));
        item.put("currency", s(saga.amount().currency()));
        item.put("version", n(saga.version()));
        item.put("createdAt", s(saga.createdAt().toString()));
        item.put("updatedAt", s(saga.createdAt().toString()));
        item.put("GSI1PK", s("ORDER#" + saga.serviceOrderId()));
        item.put("GSI1SK", s("SAGA#" + saga.sagaId()));
        return item;
    }

    private Map<String, AttributeValue> history(
            BudgetApprovedEvent source,
            PaymentSaga saga
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + saga.sagaId()));
        item.put("SK", s("EVENT#" + saga.createdAt() + "#" + source.messageId()));
        item.put("entityType", s("HISTORY"));
        item.put("eventType", s("SAGA_STARTED"));
        item.put("messageId", s(source.messageId().toString()));
        item.put("state", s(saga.state().name()));
        item.put("occurredAt", s(saga.createdAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> outbox(
            CreatePaymentCommand command
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + command.sagaId()));
        item.put("SK", s("OUTBOX#" + command.messageId()));
        item.put("entityType", s("OUTBOX"));
        item.put("messageId", s(command.messageId().toString()));
        item.put("messageType", s("CreatePayment"));
        item.put("destination", s(paymentDestination));
        item.put("payload", s(serialize(command)));
        item.put("status", s("PENDING"));
        item.put("attempts", n(0));
        item.put("availableAt", s(command.occurredAt().toString()));
        item.put("createdAt", s(command.occurredAt().toString()));
        item.put("GSI3PK", s("OUTBOX#PENDING"));
        item.put("GSI3SK", s(command.occurredAt() + "#" + command.messageId()));
        return item;
    }

    private String serialize(CreatePaymentCommand command) {
        var envelope = Map.of(
                "schemaVersion", 1,
                "messageId", command.messageId(),
                "type", "CreatePayment",
                "occurredAt", command.occurredAt(),
                "correlationId", command.correlationId(),
                "causationId", command.causationId(),
                "sagaId", command.sagaId(),
                "serviceOrderId", command.serviceOrderId(),
                "payload", Map.of(
                        "amount", Map.of(
                                "amount", command.amount().serializedAmount(),
                                "currency", command.amount().currency()
                        ),
                        "description", command.description(),
                        "idempotencyKey", command.idempotencyKey()
                )
        );
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JacksonException exception) {
            throw new IllegalStateException(
                    "Could not serialize CreatePayment command",
                    exception
            );
        }
    }

    private Map<String, AttributeValue> inboxKey(BudgetApprovedEvent event) {
        return key("MESSAGE#" + event.messageId(), "INBOX");
    }

    private Map<String, AttributeValue> activeSagaKey(PaymentSaga saga) {
        return key("ORDER#" + saga.serviceOrderId(), "ACTIVE_SAGA");
    }

    private Map<String, AttributeValue> key(String pk, String sk) {
        return Map.of("PK", s(pk), "SK", s(sk));
    }

    private AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private AttributeValue n(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }
}
