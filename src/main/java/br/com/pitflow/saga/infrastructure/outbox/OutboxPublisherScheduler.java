package br.com.pitflow.saga.infrastructure.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class OutboxPublisherScheduler {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final DynamoDbOutboxRepository repository;
    private final SqsClient sqs;
    private final Clock clock;
    private final int batchSize;
    private final Duration lease;
    private final int maxBackoffSeconds;
    private final ConcurrentHashMap<String, String> queueUrls =
            new ConcurrentHashMap<>();

    public OutboxPublisherScheduler(
            DynamoDbOutboxRepository repository,
            SqsClient sqs,
            Clock clock,
            int batchSize,
            Duration lease,
            int maxBackoffSeconds
    ) {
        this.repository = repository;
        this.sqs = sqs;
        this.clock = clock;
        this.batchSize = batchSize;
        this.lease = lease;
        this.maxBackoffSeconds = maxBackoffSeconds;
    }

    @Scheduled(
            fixedDelayString = "${orchestrator.outbox.delay-ms:5000}"
    )
    public void publishPending() {
        var now = clock.instant();
        var lockId = UUID.randomUUID();
        var messages = repository.claimBatch(
                batchSize, lockId, now, now.plus(lease)
        );
        messages.forEach(message -> publish(message, lockId));
    }

    void publish(
            DynamoDbOutboxRepository.OutboxItem message,
            UUID lockId
    ) {
        try {
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl(message.destination()))
                    .messageBody(message.payload())
                    .build());
            repository.markPublished(
                    message, lockId, clock.instant()
            );
            LOGGER.info(
                    "Outbox published messageId={}",
                    message.messageId()
            );
        } catch (RuntimeException exception) {
            var delay = Math.min(
                    maxBackoffSeconds,
                    1L << Math.min(message.attempts(), 20)
            );
            repository.releaseForRetry(
                    message,
                    lockId,
                    clock.instant().plusSeconds(delay),
                    exception.getClass().getSimpleName()
            );
            LOGGER.warn(
                    "Outbox publication failed messageId={} attempt={}",
                    message.messageId(),
                    message.attempts() + 1
            );
        }
    }

    private String queueUrl(String destination) {
        return queueUrls.computeIfAbsent(
                destination,
                name -> sqs.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(name)
                        .build()).queueUrl()
        );
    }
}
