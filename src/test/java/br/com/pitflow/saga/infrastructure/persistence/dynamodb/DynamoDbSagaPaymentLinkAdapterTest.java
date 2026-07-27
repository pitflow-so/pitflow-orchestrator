package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.event.MarkServiceOrderAwaitingPaymentCommand;
import br.com.pitflow.saga.core.event.PaymentLinkCreatedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentLinkGateway;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSagaPaymentLinkAdapterTest {
    private final DynamoDbClient client = mock(DynamoDbClient.class);
    private final DynamoDbSagaPaymentLinkAdapter adapter =
            new DynamoDbSagaPaymentLinkAdapter(
                    client,
                    new ObjectMapper(),
                    "pitflow-orchestrator",
                    "operation-command-queue"
            );

    @Test
    void updatesSagaAndWritesInboxHistoryAndOutboxAtomically() {
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        var fixture = fixture();

        var result = adapter.handleAtomically(
                fixture.event(), fixture.command()
        );

        var captor = ArgumentCaptor.forClass(
                TransactWriteItemsRequest.class
        );
        verify(client).transactWriteItems(captor.capture());
        var writes = captor.getValue().transactItems();
        assertEquals(4, writes.size());
        assertNotNull(writes.get(1).update());
        assertTrue(writes.get(1).update().conditionExpression()
                .contains("#state = :expected"));
        var outbox = writes.get(3).put().item();
        assertEquals("operation-command-queue",
                outbox.get("destination").s());
        assertTrue(outbox.get("payload").s()
                .contains("\"type\":\"MarkServiceOrderAwaitingPayment\""));
        assertTrue(outbox.get("payload").s()
                .contains(fixture.event().checkoutUrl()));
        assertEquals(SagaPaymentLinkGateway.HandleResult.UPDATED, result);
    }

    @Test
    void treatsExistingInboxAsIdempotentSuccess() {
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(TransactionCanceledException.builder()
                        .message("conditional check failed")
                        .build());
        when(client.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder()
                        .item(Map.of(
                                "PK",
                                AttributeValue.builder().s("MESSAGE#existing").build()
                        ))
                        .build());
        var fixture = fixture();

        var result = adapter.handleAtomically(
                fixture.event(), fixture.command()
        );

        assertEquals(
                SagaPaymentLinkGateway.HandleResult.ALREADY_PROCESSED,
                result
        );
    }

    private Fixture fixture() {
        var occurredAt = Instant.parse("2026-07-27T03:00:00Z");
        var event = new PaymentLinkCreatedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                "pref-1",
                "https://sandbox.mercadopago.com/checkout",
                Instant.parse("2026-07-28T03:00:00Z"),
                occurredAt
        );
        var command = new MarkServiceOrderAwaitingPaymentCommand(
                UUID.randomUUID(),
                event.correlationId(),
                event.messageId(),
                event.sagaId(),
                event.serviceOrderId(),
                event.paymentId(),
                event.preferenceId(),
                event.checkoutUrl(),
                event.expiresAt(),
                occurredAt.plusSeconds(1)
        );
        return new Fixture(event, command);
    }

    private record Fixture(
            PaymentLinkCreatedEvent event,
            MarkServiceOrderAwaitingPaymentCommand command
    ) {
    }
}
