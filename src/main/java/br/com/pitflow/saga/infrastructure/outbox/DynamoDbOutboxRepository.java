package br.com.pitflow.saga.infrastructure.outbox;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.*;

public class DynamoDbOutboxRepository {
    private final DynamoDbClient client;
    private final String tableName;

    public DynamoDbOutboxRepository(
            DynamoDbClient client,
            String tableName
    ) {
        this.client = client;
        this.tableName = tableName;
    }

    public List<OutboxItem> claimBatch(
            int batchSize,
            UUID lockId,
            Instant now,
            Instant lockedUntil
    ) {
        var response = client.query(QueryRequest.builder()
                .tableName(tableName)
                .indexName("outbox-by-status")
                .keyConditionExpression(
                        "GSI3PK = :pending AND GSI3SK <= :now"
                )
                .expressionAttributeValues(Map.of(
                        ":pending", s("OUTBOX#PENDING"),
                        ":now", s(now + "\uffff")
                ))
                .limit(batchSize)
                .build());
        var claimed = new ArrayList<OutboxItem>();
        for (var item : response.items()) {
            var candidate = toItem(item);
            if (tryClaim(candidate, lockId, now, lockedUntil)) {
                claimed.add(candidate);
            }
        }
        return claimed;
    }

    public void markPublished(
            OutboxItem item,
            UUID lockId,
            Instant publishedAt
    ) {
        client.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(item.key())
                .conditionExpression(
                        "#status = :processing AND lockId = :lockId"
                )
                .updateExpression(
                        "SET #status = :published, publishedAt = :now "
                                + "REMOVE lockId, lockedUntil, GSI3PK, GSI3SK"
                )
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(
                        ":processing", s("PROCESSING"),
                        ":published", s("PUBLISHED"),
                        ":lockId", s(lockId.toString()),
                        ":now", s(publishedAt.toString())
                ))
                .build());
    }

    public void releaseForRetry(
            OutboxItem item,
            UUID lockId,
            Instant availableAt,
            String sanitizedError
    ) {
        client.updateItem(UpdateItemRequest.builder()
                .tableName(tableName)
                .key(item.key())
                .conditionExpression(
                        "#status = :processing AND lockId = :lockId"
                )
                .updateExpression(
                        "SET #status = :pending, attempts = :attempts, "
                                + "availableAt = :availableAt, "
                                + "lastError = :lastError, GSI3SK = :gsi3sk "
                                + "REMOVE lockId, lockedUntil"
                )
                .expressionAttributeNames(Map.of("#status", "status"))
                .expressionAttributeValues(Map.of(
                        ":processing", s("PROCESSING"),
                        ":pending", s("PENDING"),
                        ":lockId", s(lockId.toString()),
                        ":attempts", n(item.attempts() + 1),
                        ":availableAt", s(availableAt.toString()),
                        ":lastError", s(sanitizedError),
                        ":gsi3sk", s(availableAt + "#" + item.messageId())
                ))
                .build());
    }

    private boolean tryClaim(
            OutboxItem item,
            UUID lockId,
            Instant now,
            Instant lockedUntil
    ) {
        try {
            client.updateItem(UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(item.key())
                    .conditionExpression(
                            "(#status = :pending OR "
                                    + "(#status = :processing "
                                    + "AND lockedUntil < :now)) "
                                    + "AND availableAt <= :now"
                    )
                    .updateExpression(
                            "SET #status = :processing, lockId = :lockId, "
                                    + "lockedUntil = :lockedUntil"
                    )
                    .expressionAttributeNames(Map.of("#status", "status"))
                    .expressionAttributeValues(Map.of(
                            ":pending", s("PENDING"),
                            ":processing", s("PROCESSING"),
                            ":now", s(now.toString()),
                            ":lockId", s(lockId.toString()),
                            ":lockedUntil", s(lockedUntil.toString())
                    ))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException exception) {
            return false;
        }
    }

    private OutboxItem toItem(Map<String, AttributeValue> item) {
        return new OutboxItem(
                item.get("PK").s(),
                item.get("SK").s(),
                UUID.fromString(item.get("messageId").s()),
                item.get("destination").s(),
                item.get("payload").s(),
                Integer.parseInt(item.get("attempts").n())
        );
    }

    private AttributeValue s(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private AttributeValue n(int value) {
        return AttributeValue.builder()
                .n(Integer.toString(value))
                .build();
    }

    public record OutboxItem(
            String pk,
            String sk,
            UUID messageId,
            String destination,
            String payload,
            int attempts
    ) {
        Map<String, AttributeValue> key() {
            return Map.of(
                    "PK", AttributeValue.builder().s(pk).build(),
                    "SK", AttributeValue.builder().s(sk).build()
            );
        }
    }
}
