package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.event.CancelServiceOrderCommand;
import br.com.pitflow.saga.core.event.PaymentRejectedEvent;
import br.com.pitflow.saga.core.event.ServiceOrderCancelledEvent;
import br.com.pitflow.saga.core.gateway.SagaCompensationGateway;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSagaCompensationAdapterTest {
    private final DynamoDbClient client = mock(DynamoDbClient.class);
    private final DynamoDbSagaCompensationAdapter adapter = new DynamoDbSagaCompensationAdapter(
            client, new ObjectMapper(), "pitflow-orchestrator", "operation-command-queue");

    @Test
    void startsCompensationAndPublishesCancellationAtomically() {
        var fixture = fixture();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        var result = adapter.startAtomically(fixture.rejected(), fixture.cancel());

        var captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        var writes = captor.getValue().transactItems();
        assertEquals(3, writes.size());
        assertEquals("COMPENSATING", writes.get(1).update().expressionAttributeValues().get(":next").s());
        var outbox = writes.get(2).put().item();
        assertEquals("operation-command-queue", outbox.get("destination").s());
        assertTrue(outbox.get("payload").s().contains("\"type\":\"CancelServiceOrder\""));
        assertEquals(SagaCompensationGateway.HandleResult.UPDATED, result);
    }

    @Test
    void marksSagaFailedAfterCancellationConfirmation() {
        var fixture = fixture();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());

        assertEquals(SagaCompensationGateway.HandleResult.UPDATED,
                adapter.failAtomically(fixture.cancelled()));

        var captor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
        verify(client).transactWriteItems(captor.capture());
        var update = captor.getValue().transactItems().get(1).update();
        assertEquals("COMPENSATING", update.expressionAttributeValues().get(":expected").s());
        assertEquals("FAILED", update.expressionAttributeValues().get(":failed").s());
    }

    @Test
    void handlesDuplicateAndPropagatesRealConflicts() {
        var fixture = fixture();
        var conflict = TransactionCanceledException.builder().message("conflict").build();
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class))).thenThrow(conflict);
        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder()
                .item(Map.of("PK", AttributeValue.builder().s("existing").build())).build());
        assertEquals(SagaCompensationGateway.HandleResult.ALREADY_PROCESSED,
                adapter.startAtomically(fixture.rejected(), fixture.cancel()));
        assertEquals(SagaCompensationGateway.HandleResult.ALREADY_PROCESSED,
                adapter.failAtomically(fixture.cancelled()));

        when(client.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().build());
        assertSame(conflict, assertThrows(TransactionCanceledException.class,
                () -> adapter.failAtomically(fixture.cancelled())));
    }

    private Fixture fixture() {
        Instant now = Instant.parse("2026-07-27T13:00:00Z");
        UUID saga = UUID.randomUUID();
        UUID order = UUID.randomUUID();
        UUID payment = UUID.randomUUID();
        var rejected = new PaymentRejectedEvent(UUID.randomUUID(), UUID.randomUUID(), saga, order, payment,
                "DECLINED", "Pagamento recusado", now);
        var cancel = new CancelServiceOrderCommand(UUID.randomUUID(), rejected.correlationId(),
                rejected.messageId(), saga, order, payment, rejected.reason(), now.plusSeconds(1));
        var cancelled = new ServiceOrderCancelledEvent(UUID.randomUUID(), rejected.correlationId(),
                cancel.messageId(), saga, order, payment, rejected.reason(), now.plusSeconds(2));
        return new Fixture(rejected, cancel, cancelled);
    }

    private record Fixture(PaymentRejectedEvent rejected, CancelServiceOrderCommand cancel,
                           ServiceOrderCancelledEvent cancelled) {}
}
