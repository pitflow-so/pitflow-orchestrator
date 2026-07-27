package br.com.pitflow.saga.infrastructure.consumer.sqs;

import br.com.pitflow.saga.controller.SagaEventController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Instant;
import java.util.UUID;

public class ServiceOrderEventConsumer {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ServiceOrderEventConsumer.class);

    private final SqsClient sqs;
    private final ObjectMapper objectMapper;
    private final SagaEventController controller;
    private final String queueUrl;
    private final int waitTimeSeconds;

    public ServiceOrderEventConsumer(
            SqsClient sqs,
            ObjectMapper objectMapper,
            SagaEventController controller,
            String queueName,
            int waitTimeSeconds
    ) {
        this.sqs = sqs;
        this.objectMapper = objectMapper;
        this.controller = controller;
        this.waitTimeSeconds = waitTimeSeconds;
        this.queueUrl = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build()).queueUrl();
    }

    @Scheduled(
            fixedDelayString = "${orchestrator.consumer.delay-ms:1000}"
    )
    public void poll() {
        var response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(waitTimeSeconds)
                .maxNumberOfMessages(10)
                .build());
        response.messages().forEach(this::process);
    }

    void process(Message message) {
        try {
            var root = objectMapper.readTree(message.body());
            validateEnvelope(root);
            if (!"ServiceOrderBudgetApproved".equals(
                    root.path("type").asText()
            )) {
                throw new java.lang.UnsupportedOperationException(
                        "Unsupported message type"
                );
            }
            var amount = root.path("payload").path("amount");
            var result = controller.budgetApproved(
                    new SagaEventController.BudgetApprovedCommand(
                            UUID.fromString(root.path("messageId").asText()),
                            UUID.fromString(
                                    root.path("correlationId").asText()
                            ),
                            UUID.fromString(
                                    root.path("serviceOrderId").asText()
                            ),
                            requiredText(amount, "amount"),
                            requiredText(amount, "currency"),
                            Instant.parse(
                                    root.path("occurredAt").asText()
                            )
                    )
            );
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
            LOGGER.info(
                    "SAGA start handled messageId={} result={}",
                    root.path("messageId").asText(),
                    result
            );
        } catch (RuntimeException exception) {
            LOGGER.warn(
                    "SAGA event processing failed sqsMessageId={}",
                    message.messageId(),
                    exception
            );
        }
    }

    private void validateEnvelope(JsonNode root) {
        if (root.path("schemaVersion").asInt() != 1) {
            throw new IllegalArgumentException(
                    "Unsupported schema version"
            );
        }
        requiredText(root, "messageId");
        requiredText(root, "correlationId");
        requiredText(root, "serviceOrderId");
        requiredText(root, "occurredAt");
        requiredText(root, "type");
    }

    private String requiredText(JsonNode node, String field) {
        var value = node.path(field).asText();
        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    "Required field is missing: " + field
            );
        }
        return value;
    }
}
