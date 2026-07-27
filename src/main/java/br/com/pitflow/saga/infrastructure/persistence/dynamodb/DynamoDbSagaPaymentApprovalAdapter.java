package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.SagaState;
import br.com.pitflow.saga.core.event.MarkServiceOrderReadyForExecutionCommand;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public class DynamoDbSagaPaymentApprovalAdapter
        implements SagaPaymentApprovalGateway {
    private static final String ITEM_ABSENT =
            "attribute_not_exists(PK) AND attribute_not_exists(SK)";

    private final DynamoDbClient client;
    private final ObjectMapper objectMapper;
    private final String tableName;
    private final String operationDestination;

    public DynamoDbSagaPaymentApprovalAdapter(
            DynamoDbClient client,
            ObjectMapper objectMapper,
            String tableName,
            String operationDestination
    ) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.tableName = tableName;
        this.operationDestination = operationDestination;
    }

    @Override
    public HandleResult handleAtomically(
            PaymentApprovedEvent source,
            MarkServiceOrderReadyForExecutionCommand command
    ) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            put(inbox(source)),
                            updateSaga(source, command),
                            put(history(source)),
                            put(outbox(command))
                    )
                    .build());
            return HandleResult.UPDATED;
        } catch (TransactionCanceledException exception) {
            if (alreadyExists(inboxKey(source))) {
                return HandleResult.ALREADY_PROCESSED;
            }
            throw exception;
        }
    }

    @Override
    public HandleResult completeAtomically(
            ServiceOrderReadyForExecutionEvent source
    ) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            put(completionInbox(source)),
                            completeSaga(source),
                            put(completionHistory(source))
                    )
                    .build());
            return HandleResult.UPDATED;
        } catch (TransactionCanceledException exception) {
            if (alreadyExists(completionInboxKey(source))) {
                return HandleResult.ALREADY_PROCESSED;
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

    private TransactWriteItem updateSaga(
            PaymentApprovedEvent source,
            MarkServiceOrderReadyForExecutionCommand command
    ) {
        return TransactWriteItem.builder()
                .update(Update.builder()
                        .tableName(tableName)
                        .key(key("SAGA#" + source.sagaId(), "METADATA"))
                        .conditionExpression(
                                "#state = :expected "
                                        + "AND serviceOrderId = :order "
                                        + "AND paymentId = :paymentId"
                        )
                        .updateExpression(
                                "SET #state = :next, "
                                        + "externalPaymentId = :externalPaymentId, "
                                        + "paymentApprovedAt = :approvedAt, "
                                        + "updatedAt = :updatedAt, "
                                        + "#version = #version + :one"
                        )
                        .expressionAttributeNames(Map.of(
                                "#state", "state",
                                "#version", "version"
                        ))
                        .expressionAttributeValues(Map.of(
                                ":expected", s(SagaState.AWAITING_PAYMENT.name()),
                                ":next", s(SagaState.RELEASING_SERVICE_ORDER.name()),
                                ":order", s(source.serviceOrderId().toString()),
                                ":paymentId", s(source.paymentId().toString()),
                                ":externalPaymentId", s(source.externalPaymentId()),
                                ":approvedAt", s(source.occurredAt().toString()),
                                ":updatedAt", s(command.occurredAt().toString()),
                                ":one", n(1)
                        ))
                        .build())
                .build();
    }

    private TransactWriteItem completeSaga(
            ServiceOrderReadyForExecutionEvent source
    ) {
        return TransactWriteItem.builder()
                .update(Update.builder()
                        .tableName(tableName)
                        .key(key("SAGA#" + source.sagaId(), "METADATA"))
                        .conditionExpression(
                                "#state = :expected "
                                        + "AND serviceOrderId = :order "
                                        + "AND paymentId = :paymentId"
                        )
                        .updateExpression(
                                "SET #state = :next, completedAt = :completedAt, "
                                        + "updatedAt = :completedAt, "
                                        + "#version = #version + :one"
                        )
                        .expressionAttributeNames(Map.of(
                                "#state", "state",
                                "#version", "version"
                        ))
                        .expressionAttributeValues(Map.of(
                                ":expected", s(SagaState.RELEASING_SERVICE_ORDER.name()),
                                ":next", s(SagaState.COMPLETED.name()),
                                ":order", s(source.serviceOrderId().toString()),
                                ":paymentId", s(source.paymentId().toString()),
                                ":completedAt", s(source.occurredAt().toString()),
                                ":one", n(1)
                        ))
                        .build())
                .build();
    }

    private Map<String, AttributeValue> inbox(PaymentApprovedEvent event) {
        var item = new LinkedHashMap<>(inboxKey(event));
        item.put("entityType", s("INBOX"));
        item.put("messageType", s("PaymentApproved"));
        item.put("sagaId", s(event.sagaId().toString()));
        item.put("serviceOrderId", s(event.serviceOrderId().toString()));
        item.put("processedAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> history(PaymentApprovedEvent event) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + event.sagaId()));
        item.put("SK", s("EVENT#" + event.occurredAt() + "#" + event.messageId()));
        item.put("entityType", s("HISTORY"));
        item.put("eventType", s("PAYMENT_APPROVED"));
        item.put("messageId", s(event.messageId().toString()));
        item.put("paymentId", s(event.paymentId().toString()));
        item.put("externalPaymentId", s(event.externalPaymentId()));
        item.put("state", s(SagaState.RELEASING_SERVICE_ORDER.name()));
        item.put("occurredAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> completionInbox(
            ServiceOrderReadyForExecutionEvent event
    ) {
        var item = new LinkedHashMap<>(completionInboxKey(event));
        item.put("entityType", s("INBOX"));
        item.put("messageType", s("ServiceOrderReadyForExecution"));
        item.put("sagaId", s(event.sagaId().toString()));
        item.put("serviceOrderId", s(event.serviceOrderId().toString()));
        item.put("processedAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> completionHistory(
            ServiceOrderReadyForExecutionEvent event
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + event.sagaId()));
        item.put("SK", s("EVENT#" + event.occurredAt() + "#" + event.messageId()));
        item.put("entityType", s("HISTORY"));
        item.put("eventType", s("SERVICE_ORDER_READY_FOR_EXECUTION"));
        item.put("messageId", s(event.messageId().toString()));
        item.put("paymentId", s(event.paymentId().toString()));
        item.put("state", s(SagaState.COMPLETED.name()));
        item.put("occurredAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> outbox(
            MarkServiceOrderReadyForExecutionCommand command
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + command.sagaId()));
        item.put("SK", s("OUTBOX#" + command.messageId()));
        item.put("entityType", s("OUTBOX"));
        item.put("messageId", s(command.messageId().toString()));
        item.put("messageType", s("MarkServiceOrderReadyForExecution"));
        item.put("destination", s(operationDestination));
        item.put("payload", s(serialize(command)));
        item.put("status", s("PENDING"));
        item.put("attempts", n(0));
        item.put("availableAt", s(command.occurredAt().toString()));
        item.put("createdAt", s(command.occurredAt().toString()));
        item.put("GSI3PK", s("OUTBOX#PENDING"));
        item.put("GSI3SK", s(command.occurredAt() + "#" + command.messageId()));
        return item;
    }

    private String serialize(
            MarkServiceOrderReadyForExecutionCommand command
    ) {
        var envelope = Map.of(
                "schemaVersion", 1,
                "messageId", command.messageId(),
                "type", "MarkServiceOrderReadyForExecution",
                "occurredAt", command.occurredAt(),
                "correlationId", command.correlationId(),
                "causationId", command.causationId(),
                "sagaId", command.sagaId(),
                "serviceOrderId", command.serviceOrderId(),
                "payload", Map.of(
                        "paymentId", command.paymentId(),
                        "externalPaymentId", command.externalPaymentId()
                )
        );
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JacksonException exception) {
            throw new IllegalStateException(
                    "Could not serialize operation command",
                    exception
            );
        }
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

    private Map<String, AttributeValue> inboxKey(PaymentApprovedEvent event) {
        return key("MESSAGE#" + event.messageId(), "INBOX");
    }

    private Map<String, AttributeValue> completionInboxKey(
            ServiceOrderReadyForExecutionEvent event
    ) {
        return key("MESSAGE#" + event.messageId(), "INBOX");
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
