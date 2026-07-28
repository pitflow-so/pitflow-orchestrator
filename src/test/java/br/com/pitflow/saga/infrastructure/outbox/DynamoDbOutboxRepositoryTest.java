package br.com.pitflow.saga.infrastructure.outbox;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DynamoDbOutboxRepositoryTest {
    private final DynamoDbClient client = mock(DynamoDbClient.class);
    private final DynamoDbOutboxRepository repository =
            new DynamoDbOutboxRepository(client, "pitflow-orchestrator");

    @Test
    void queriesEligibleItemsAndClaimsOnlyAvailableCandidates() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        when(client.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder()
                .items(item(first, 0), item(second, 2)).build());
        when(client.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build())
                .thenThrow(ConditionalCheckFailedException.builder().message("already claimed").build());
        Instant now = Instant.parse("2026-07-27T14:00:00Z");

        List<DynamoDbOutboxRepository.OutboxItem> result = repository.claimBatch(
                10, UUID.randomUUID(), now, now.plusSeconds(60));

        assertEquals(1, result.size());
        assertEquals(first, result.getFirst().messageId());
        var query = ArgumentCaptor.forClass(QueryRequest.class);
        verify(client).query(query.capture());
        assertEquals("outbox-by-status", query.getValue().indexName());
        assertEquals(10, query.getValue().limit());
    }

    @Test
    void marksClaimedItemAsPublished() {
        var item = outboxItem();
        UUID lock = UUID.randomUUID();
        repository.markPublished(item, lock, Instant.parse("2026-07-27T14:05:00Z"));

        var captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(captor.capture());
        assertTrue(captor.getValue().updateExpression().contains(":published"));
        assertEquals("PUBLISHED", captor.getValue().expressionAttributeValues().get(":published").s());
        assertEquals(lock.toString(), captor.getValue().expressionAttributeValues().get(":lockId").s());
    }

    @Test
    void releasesClaimedItemForRetryAndIncrementsAttempts() {
        var item = outboxItem();
        UUID lock = UUID.randomUUID();
        Instant retryAt = Instant.parse("2026-07-27T14:10:00Z");
        repository.releaseForRetry(item, lock, retryAt, "temporary failure");

        var captor = ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(client).updateItem(captor.capture());
        var values = captor.getValue().expressionAttributeValues();
        assertEquals("PENDING", values.get(":pending").s());
        assertEquals("4", values.get(":attempts").n());
        assertEquals("temporary failure", values.get(":lastError").s());
        assertTrue(values.get(":gsi3sk").s().startsWith(retryAt.toString()));
    }

    private Map<String, AttributeValue> item(UUID id, int attempts) {
        return Map.of(
                "PK", s("SAGA#" + UUID.randomUUID()),
                "SK", s("OUTBOX#" + id),
                "messageId", s(id.toString()),
                "destination", s("queue"),
                "payload", s("{}"),
                "attempts", AttributeValue.builder().n(Integer.toString(attempts)).build());
    }

    private DynamoDbOutboxRepository.OutboxItem outboxItem() {
        UUID id = UUID.randomUUID();
        return new DynamoDbOutboxRepository.OutboxItem("SAGA#" + UUID.randomUUID(),
                "OUTBOX#" + id, id, "queue", "{}", 3);
    }

    private AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }
}
