package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.SagaState;
import br.com.pitflow.saga.core.event.*;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public final class DynamoDbSagaCompensationAdapter implements SagaCompensationGateway {
    private final DynamoDbClient client;
    private final ObjectMapper mapper;
    private final String table;
    private final String destination;

    public DynamoDbSagaCompensationAdapter(DynamoDbClient client, ObjectMapper mapper,
                                           String table, String destination) {
        this.client = client; this.mapper = mapper; this.table = table; this.destination = destination;
    }

    @Override
    public HandleResult startAtomically(PaymentRejectedEvent source, CancelServiceOrderCommand command) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                    put(inbox(source.messageId(), "PaymentRejected", source.sagaId(), source.serviceOrderId(), source.occurredAt().toString())),
                    update(source, command), put(outbox(command))).build());
            return HandleResult.UPDATED;
        } catch (TransactionCanceledException e) {
            if (exists(source.messageId())) return HandleResult.ALREADY_PROCESSED;
            throw e;
        }
    }

    @Override
    public HandleResult failAtomically(ServiceOrderCancelledEvent source) {
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                    put(inbox(source.messageId(), "ServiceOrderCancelled", source.sagaId(), source.serviceOrderId(), source.occurredAt().toString())),
                    TransactWriteItem.builder().update(Update.builder().tableName(table)
                            .key(key("SAGA#" + source.sagaId(), "METADATA"))
                            .conditionExpression("#s = :expected AND paymentId = :payment")
                            .updateExpression("SET #s = :failed, failedAt = :at, updatedAt = :at, #v = #v + :one")
                            .expressionAttributeNames(Map.of("#s","state","#v","version"))
                            .expressionAttributeValues(Map.of(":expected",s(SagaState.COMPENSATING.name()),
                                    ":failed",s(SagaState.FAILED.name()),":payment",s(source.paymentId().toString()),
                                    ":at",s(source.occurredAt().toString()),":one",n(1))).build()).build()).build());
            return HandleResult.UPDATED;
        } catch (TransactionCanceledException e) {
            if (exists(source.messageId())) return HandleResult.ALREADY_PROCESSED;
            throw e;
        }
    }

    private TransactWriteItem update(PaymentRejectedEvent source, CancelServiceOrderCommand command) {
        return TransactWriteItem.builder().update(Update.builder().tableName(table)
                .key(key("SAGA#" + source.sagaId(), "METADATA"))
                .conditionExpression("#s = :expected AND paymentId = :payment")
                .updateExpression("SET #s = :next, failureReason = :reason, updatedAt = :at, #v = #v + :one")
                .expressionAttributeNames(Map.of("#s","state","#v","version"))
                .expressionAttributeValues(Map.of(":expected",s(SagaState.AWAITING_PAYMENT.name()),
                        ":next",s(SagaState.COMPENSATING.name()),":payment",s(source.paymentId().toString()),
                        ":reason",s(source.reason()),":at",s(command.occurredAt().toString()),":one",n(1))).build()).build();
    }

    private Map<String,AttributeValue> outbox(CancelServiceOrderCommand c) {
        var item = new LinkedHashMap<String,AttributeValue>();
        item.put("PK",s("SAGA#"+c.sagaId())); item.put("SK",s("OUTBOX#"+c.messageId()));
        item.put("entityType",s("OUTBOX")); item.put("messageId",s(c.messageId().toString()));
        item.put("messageType",s("CancelServiceOrder")); item.put("destination",s(destination));
        item.put("payload",s(serialize(c))); item.put("status",s("PENDING")); item.put("attempts",n(0));
        item.put("availableAt",s(c.occurredAt().toString())); item.put("createdAt",s(c.occurredAt().toString()));
        item.put("GSI3PK",s("OUTBOX#PENDING")); item.put("GSI3SK",s(c.occurredAt()+"#"+c.messageId()));
        return item;
    }

    private String serialize(CancelServiceOrderCommand c) {
        try {
            return mapper.writeValueAsString(Map.of("schemaVersion",1,"messageId",c.messageId(),
                    "type","CancelServiceOrder","occurredAt",c.occurredAt(),"correlationId",c.correlationId(),
                    "causationId",c.causationId(),"sagaId",c.sagaId(),"serviceOrderId",c.serviceOrderId(),
                    "payload",Map.of("paymentId",c.paymentId(),"reason",c.reason())));
        } catch (Exception e) { throw new IllegalStateException("Could not serialize compensation", e); }
    }

    private Map<String,AttributeValue> inbox(java.util.UUID id, String type, java.util.UUID saga,
                                             java.util.UUID order, String at) {
        var item = new LinkedHashMap<>(key("MESSAGE#"+id,"INBOX"));
        item.put("entityType",s("INBOX")); item.put("messageType",s(type));
        item.put("sagaId",s(saga.toString())); item.put("serviceOrderId",s(order.toString()));
        item.put("processedAt",s(at)); return item;
    }
    private TransactWriteItem put(Map<String,AttributeValue> item) { return TransactWriteItem.builder().put(
            Put.builder().tableName(table).item(item).conditionExpression("attribute_not_exists(PK) AND attribute_not_exists(SK)").build()).build(); }
    private boolean exists(java.util.UUID id) { return client.getItem(GetItemRequest.builder().tableName(table)
            .key(key("MESSAGE#"+id,"INBOX")).consistentRead(true).build()).hasItem(); }
    private Map<String,AttributeValue> key(String pk,String sk){return Map.of("PK",s(pk),"SK",s(sk));}
    private AttributeValue s(String v){return AttributeValue.builder().s(v).build();}
    private AttributeValue n(long v){return AttributeValue.builder().n(Long.toString(v)).build();}
}
