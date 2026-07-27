package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.SagaState;
import br.com.pitflow.saga.core.event.MarkServiceOrderAwaitingPaymentCommand;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderAwaitingPaymentEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class DynamoDbSagaPaymentLinkAdapter
        implements SagaPaymentLinkGateway {
    private static final String ITEM_ABSENT =
            "attribute_not_exists(PK) AND attribute_not_exists(SK)";

    private final DynamoDbClient client;
    private final ObjectMapper objectMapper;
    private final String tableName;
    private final String operationDestination;

    public DynamoDbSagaPaymentLinkAdapter(
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
    public HandleResult confirmAwaitingPaymentAtomically(
            ServiceOrderAwaitingPaymentEvent source
    ) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            put(confirmationInbox(source)),
                            confirmSaga(source),
                            put(confirmationHistory(source))
                    )
                    .build());
            return HandleResult.UPDATED;
        } catch (TransactionCanceledException exception) {
            if (alreadyExists(confirmationInboxKey(source))) {
                return HandleResult.ALREADY_PROCESSED;
            }
            throw exception;
        }
    }

    @Override
    public HandleResult handleAtomically(
            PaymentLinkCreatedEvent source,
            MarkServiceOrderAwaitingPaymentCommand command
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
            PaymentLinkCreatedEvent source,
            MarkServiceOrderAwaitingPaymentCommand command
    ) {
        return TransactWriteItem.builder()
                .update(Update.builder()
                        .tableName(tableName)
                        .key(key("SAGA#" + source.sagaId(), "METADATA"))
                        .conditionExpression(
                                "#state = :expected AND serviceOrderId = :order"
                        )
                        .updateExpression(
                                "SET #state = :next, paymentId = :paymentId, "
                                        + "preferenceId = :preferenceId, "
                                        + "checkoutUrl = :checkoutUrl, "
                                        + "paymentExpiresAt = :expiresAt, "
                                        + "updatedAt = :updatedAt, "
                                        + "#version = #version + :one"
                        )
                        .expressionAttributeNames(Map.of(
                                "#state", "state",
                                "#version", "version"
                        ))
                        .expressionAttributeValues(Map.of(
                                ":expected", s(SagaState.PAYMENT_CREATION_PENDING.name()),
                                ":next", s(SagaState.AWAITING_PAYMENT.name()),
                                ":order", s(source.serviceOrderId().toString()),
                                ":paymentId", s(source.paymentId().toString()),
                                ":preferenceId", s(source.preferenceId()),
                                ":checkoutUrl", s(source.checkoutUrl()),
                                ":expiresAt", s(source.expiresAt().toString()),
                                ":updatedAt", s(command.occurredAt().toString()),
                                ":one", n(1)
                        ))
                        .build())
                .build();
    }

    private TransactWriteItem confirmSaga(
            ServiceOrderAwaitingPaymentEvent source
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
                                "SET operationConfirmedAt = :confirmedAt, "
                                        + "updatedAt = :confirmedAt, "
                                        + "#version = #version + :one"
                        )
                        .expressionAttributeNames(Map.of(
                                "#state", "state",
                                "#version", "version"
                        ))
                        .expressionAttributeValues(Map.of(
                                ":expected", s(SagaState.AWAITING_PAYMENT.name()),
                                ":order", s(source.serviceOrderId().toString()),
                                ":paymentId", s(source.paymentId().toString()),
                                ":confirmedAt", s(source.occurredAt().toString()),
                                ":one", n(1)
                        ))
                        .build())
                .build();
    }

    private Map<String, AttributeValue> inbox(
            PaymentLinkCreatedEvent event
    ) {
        var item = new LinkedHashMap<>(inboxKey(event));
        item.put("entityType", s("INBOX"));
        item.put("messageType", s("PaymentLinkCreated"));
        item.put("sagaId", s(event.sagaId().toString()));
        item.put("serviceOrderId", s(event.serviceOrderId().toString()));
        item.put("processedAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> confirmationInbox(
            ServiceOrderAwaitingPaymentEvent event
    ) {
        var item = new LinkedHashMap<>(confirmationInboxKey(event));
        item.put("entityType", s("INBOX"));
        item.put("messageType", s("ServiceOrderAwaitingPayment"));
        item.put("sagaId", s(event.sagaId().toString()));
        item.put("serviceOrderId", s(event.serviceOrderId().toString()));
        item.put("processedAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> history(
            PaymentLinkCreatedEvent event
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + event.sagaId()));
        item.put("SK", s("EVENT#" + event.occurredAt() + "#" + event.messageId()));
        item.put("entityType", s("HISTORY"));
        item.put("eventType", s("PAYMENT_LINK_CREATED"));
        item.put("messageId", s(event.messageId().toString()));
        item.put("paymentId", s(event.paymentId().toString()));
        item.put("state", s(SagaState.AWAITING_PAYMENT.name()));
        item.put("occurredAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> confirmationHistory(
            ServiceOrderAwaitingPaymentEvent event
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + event.sagaId()));
        item.put("SK", s("EVENT#" + event.occurredAt() + "#" + event.messageId()));
        item.put("entityType", s("HISTORY"));
        item.put("eventType", s("SERVICE_ORDER_AWAITING_PAYMENT"));
        item.put("messageId", s(event.messageId().toString()));
        item.put("paymentId", s(event.paymentId().toString()));
        item.put("state", s(SagaState.AWAITING_PAYMENT.name()));
        item.put("occurredAt", s(event.occurredAt().toString()));
        return item;
    }

    private Map<String, AttributeValue> outbox(
            MarkServiceOrderAwaitingPaymentCommand command
    ) {
        var item = new LinkedHashMap<String, AttributeValue>();
        item.put("PK", s("SAGA#" + command.sagaId()));
        item.put("SK", s("OUTBOX#" + command.messageId()));
        item.put("entityType", s("OUTBOX"));
        item.put("messageId", s(command.messageId().toString()));
        item.put("messageType", s("MarkServiceOrderAwaitingPayment"));
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
            MarkServiceOrderAwaitingPaymentCommand command
    ) {
        var envelope = Map.of(
                "schemaVersion", 1,
                "messageId", command.messageId(),
                "type", "MarkServiceOrderAwaitingPayment",
                "occurredAt", command.occurredAt(),
                "correlationId", command.correlationId(),
                "causationId", command.causationId(),
                "sagaId", command.sagaId(),
                "serviceOrderId", command.serviceOrderId(),
                "payload", Map.of(
                        "paymentId", command.paymentId(),
                        "preferenceId", command.preferenceId(),
                        "checkoutUrl", command.checkoutUrl(),
                        "expiresAt", command.expiresAt()
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

    private Map<String, AttributeValue> inboxKey(
            PaymentLinkCreatedEvent event
    ) {
        return key("MESSAGE#" + event.messageId(), "INBOX");
    }

    private Map<String, AttributeValue> confirmationInboxKey(
            ServiceOrderAwaitingPaymentEvent event
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
