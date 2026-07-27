package br.com.pitflow.saga.infrastructure.outbox;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.*;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OutboxPublisherSchedulerTest {
    private final DynamoDbOutboxRepository repository =
            mock(DynamoDbOutboxRepository.class);
    private final SqsClient sqs = mock(SqsClient.class);
    private final Instant now =
            Instant.parse("2026-07-26T20:00:00Z");
    private final OutboxPublisherScheduler publisher =
            new OutboxPublisherScheduler(
                    repository,
                    sqs,
                    Clock.fixed(now, ZoneOffset.UTC),
                    10,
                    Duration.ofSeconds(60),
                    300
            );

    @Test
    void marksPublishedOnlyAfterSqsAcknowledges() {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder()
                        .queueUrl("payment-url")
                        .build());
        when(sqs.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder()
                        .messageId("sqs-id")
                        .build());
        var message = message(0);
        var lockId = UUID.randomUUID();

        publisher.publish(message, lockId);

        verify(repository).markPublished(message, lockId, now);
        verify(repository, never()).releaseForRetry(
                any(), any(), any(), anyString()
        );
    }

    @Test
    void releasesWithExponentialBackoffWhenSqsFails() {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenThrow(SqsException.builder()
                        .message("unavailable")
                        .build());
        var message = message(2);
        var lockId = UUID.randomUUID();

        publisher.publish(message, lockId);

        verify(repository).releaseForRetry(
                message,
                lockId,
                now.plusSeconds(4),
                "SqsException"
        );
        verify(repository, never()).markPublished(any(), any(), any());
    }

    private DynamoDbOutboxRepository.OutboxItem message(int attempts) {
        return new DynamoDbOutboxRepository.OutboxItem(
                "SAGA#1",
                "OUTBOX#1",
                UUID.randomUUID(),
                "payment-command-queue",
                "{}",
                attempts
        );
    }
}
