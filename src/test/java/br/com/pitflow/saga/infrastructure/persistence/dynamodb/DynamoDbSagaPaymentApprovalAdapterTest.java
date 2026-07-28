package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.event.MarkServiceOrderReadyForExecutionCommand;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderReadyForExecutionEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSagaPaymentApprovalAdapterTest {
    private final DynamoDbClient client = mock(DynamoDbClient.class);
    private final DynamoDbSagaPaymentApprovalAdapter adapter = new DynamoDbSagaPaymentApprovalAdapter(
            client, new ObjectMapper(), "pitflow-orchestrator", "operation-command-queue");

    @Test
    void approvesPaymentAndCreatesOperationCommandAtomically() {
        var fixture = fixture();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        var result = adapter.handleAtomically(fixture.approved(), fixture.command());

        var captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        var writes = captor.getValue().transactItems();
        assertEquals(4, writes.size());
        assertEquals("AWAITING_PAYMENT",
                writes.get(1).update().expressionAttributeValues().get(":expected").s());
        assertEquals("RELEASING_SERVICE_ORDER",
                writes.get(1).update().expressionAttributeValues().get(":next").s());
        var outbox = writes.get(3).put().item();
        assertEquals("operation-command-queue", outbox.get("destination").s());
        assertTrue(outbox.get("payload").s().contains("\"type\":\"MarkServiceOrderReadyForExecution\""));
        assertEquals(SagaPaymentApprovalGateway.HandleResult.UPDATED, result);
    }

    @Test
    void completesSagaAfterOperationConfirmation() {
        var fixture = fixture();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        var result = adapter.completeAtomically(fixture.ready());

        var captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        var writes = captor.getValue().transactItems();
        assertEquals(3, writes.size());
        assertEquals("RELEASING_SERVICE_ORDER",
                writes.get(1).update().expressionAttributeValues().get(":expected").s());
        assertEquals("COMPLETED", writes.get(1).update().expressionAttributeValues().get(":next").s());
        assertEquals("SERVICE_ORDER_READY_FOR_EXECUTION",
                writes.get(2).put().item().get("eventType").s());
        assertEquals(SagaPaymentApprovalGateway.HandleResult.UPDATED, result);
    }

    @Test
    void treatsExistingApprovalAndCompletionInboxAsIdempotent() {
        var fixture = fixture();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(TransactionCanceledException.builder().message("conditional").build());
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder()
                .item(Map.of("PK", AttributeValue.builder().s("existing").build())).build());

        assertEquals(SagaPaymentApprovalGateway.HandleResult.ALREADY_PROCESSED,
                adapter.handleAtomically(fixture.approved(), fixture.command()));
        assertEquals(SagaPaymentApprovalGateway.HandleResult.ALREADY_PROCESSED,
                adapter.completeAtomically(fixture.ready()));
    }

    @Test
    void propagatesStateConflictWhenInboxDoesNotExist() {
        var fixture = fixture();
        var failure = TransactionCanceledException.builder().message("state conflict").build();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class))).thenThrow(failure);
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());

        assertSame(failure, assertThrows(TransactionCanceledException.class,
                () -> adapter.handleAtomically(fixture.approved(), fixture.command())));
    }

    private Fixture fixture() {
        Instant now = Instant.parse("2026-07-27T12:00:00Z");
        UUID saga = UUID.randomUUID();
        UUID order = UUID.randomUUID();
        UUID payment = UUID.randomUUID();
        var approved = new PaymentApprovedEvent(UUID.randomUUID(), UUID.randomUUID(), saga, order, payment,
                new Money(new BigDecimal("250.99"), "BRL"), "mp-123", now);
        var command = new MarkServiceOrderReadyForExecutionCommand(UUID.randomUUID(), approved.correlationId(),
                approved.messageId(), saga, order, payment, approved.externalPaymentId(), now.plusSeconds(1));
        var ready = new ServiceOrderReadyForExecutionEvent(UUID.randomUUID(), approved.correlationId(),
                command.messageId(), saga, order, payment, now.plusSeconds(2));
        return new Fixture(approved, command, ready);
    }

    private record Fixture(PaymentApprovedEvent approved,
                           MarkServiceOrderReadyForExecutionCommand command,
                           ServiceOrderReadyForExecutionEvent ready) {}
}
