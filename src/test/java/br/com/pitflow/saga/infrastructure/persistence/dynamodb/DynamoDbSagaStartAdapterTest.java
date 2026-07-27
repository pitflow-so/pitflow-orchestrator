package br.com.pitflow.saga.infrastructure.persistence.dynamodb;

import br.com.pitflow.saga.core.entity.Money;
import br.com.pitflow.saga.core.entity.PaymentSaga;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.CreatePaymentCommand;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbSagaStartAdapterTest {
    private final DynamoDbClient client = mock(DynamoDbClient.class);
    private final DynamoDbSagaStartAdapter adapter =
            new DynamoDbSagaStartAdapter(
                    client,
                    new ObjectMapper(),
                    "pitflow-orchestrator",
                    "payment-command-queue"
            );

    @Test
    void writesInboxGuardSagaHistoryAndOutboxAtomically() {
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenReturn(TransactWriteItemsResponse.builder().build());
        var fixture = fixture();

        var result = adapter.startAtomically(
                fixture.source(), fixture.saga(), fixture.command()
        );

        var captor = ArgumentCaptor.forClass(
                TransactWriteItemsRequest.class
        );
        verify(client).transactWriteItems(captor.capture());
        var writes = captor.getValue().transactItems();
        assertEquals(5, writes.size());
        assertTrue(writes.stream().allMatch(item ->
                item.put().conditionExpression()
                        .contains("attribute_not_exists")
        ));
        var outbox = writes.get(4).put().item();
        assertEquals(
                "OUTBOX#PENDING",
                outbox.get("GSI3PK").s()
        );
        assertEquals(
                "payment-command-queue",
                outbox.get("destination").s()
        );
        assertTrue(outbox.get("payload").s()
                .contains("\"type\":\"CreatePayment\""));
        assertEquals(SagaStartGateway.StartResult.CREATED, result);
    }

    @Test
    void treatsAnExistingInboxAsIdempotentSuccess() {
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
                .thenThrow(TransactionCanceledException.builder()
                        .message("conditional check failed")
                        .build());
        when(client.getItem(any(GetItemRequest.class)))
                .thenReturn(GetItemResponse.builder()
                        .item(Map.of(
                                "PK",
                                AttributeValue.builder()
                                        .s("MESSAGE#existing")
                                        .build()
                        ))
                        .build());
        var fixture = fixture();

        var result = adapter.startAtomically(
                fixture.source(), fixture.saga(), fixture.command()
        );

        assertEquals(
                SagaStartGateway.StartResult.ALREADY_PROCESSED,
                result
        );
    }

    private Fixture fixture() {
        var now = Instant.parse("2026-07-26T20:00:00Z");
        var source = new BudgetApprovedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new Money(new BigDecimal("450.00"), "BRL"),
                now
        );
        var saga = PaymentSaga.start(
                UUID.randomUUID(),
                source.serviceOrderId(),
                source.amount(),
                now
        );
        var command = new CreatePaymentCommand(
                UUID.randomUUID(),
                source.correlationId(),
                source.messageId(),
                saga.sagaId(),
                source.serviceOrderId(),
                source.amount(),
                "Pagamento da ordem de serviço",
                saga.sagaId(),
                now
        );
        return new Fixture(source, saga, command);
    }

    private record Fixture(
            BudgetApprovedEvent source,
            PaymentSaga saga,
            CreatePaymentCommand command
    ) {
    }
}
