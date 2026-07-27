package br.com.pitflow.saga.infrastructure.consumer.sqs;

import br.com.pitflow.saga.controller.SagaEventController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

public class OrchestratorEventConsumer {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(OrchestratorEventConsumer.class);

    private final SqsClient sqs;
    private final ObjectMapper objectMapper;
    private final SagaEventController controller;
    private final String queueUrl;
    private final int waitTimeSeconds;

    public OrchestratorEventConsumer(
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
            var result = dispatch(root);
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
            LOGGER.info(
                    "SAGA event handled type={} messageId={} result={}",
                    root.path("type").asText(),
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

    private Object dispatch(JsonNode root) {
        return switch (root.path("type").asText()) {
            case "ServiceOrderBudgetApproved" -> budgetApproved(root);
            case "PaymentLinkCreated" -> paymentLinkCreated(root);
            case "PaymentApproved" -> paymentApproved(root);
            case "ServiceOrderReadyForExecution" ->
                    serviceOrderReadyForExecution(root);
            case "ServiceOrderAwaitingPayment" ->
                    serviceOrderAwaitingPayment(root);
            default -> throw new java.lang.UnsupportedOperationException(
                    "Unsupported message type: "
                            + root.path("type").asText()
            );
        };
    }

    private Object serviceOrderAwaitingPayment(JsonNode root) {
        var payload = root.path("payload");
        return controller.serviceOrderAwaitingPayment(
                new SagaEventController.ServiceOrderAwaitingPaymentCommand(
                        uuid(root, "messageId"),
                        uuid(root, "correlationId"),
                        uuid(root, "causationId"),
                        uuid(root, "sagaId"),
                        uuid(root, "serviceOrderId"),
                        uuid(payload, "paymentId"),
                        instant(root, "occurredAt")
                )
        );
    }

    private Object budgetApproved(JsonNode root) {
        var amount = root.path("payload").path("amount");
        return controller.budgetApproved(
                new SagaEventController.BudgetApprovedCommand(
                        uuid(root, "messageId"),
                        uuid(root, "correlationId"),
                        uuid(root, "serviceOrderId"),
                        requiredText(amount, "amount"),
                        requiredText(amount, "currency"),
                        instant(root, "occurredAt")
                )
        );
    }

    private Object paymentLinkCreated(JsonNode root) {
        var payload = root.path("payload");
        return controller.paymentLinkCreated(
                new SagaEventController.PaymentLinkCreatedCommand(
                        uuid(root, "messageId"),
                        uuid(root, "correlationId"),
                        uuid(root, "sagaId"),
                        uuid(root, "serviceOrderId"),
                        uuid(payload, "paymentId"),
                        requiredText(payload, "preferenceId"),
                        requiredText(payload, "checkoutUrl"),
                        instant(payload, "expiresAt"),
                        instant(root, "occurredAt")
                )
        );
    }

    private Object paymentApproved(JsonNode root) {
        var payload = root.path("payload");
        var amount = payload.path("approvedAmount");
        return controller.paymentApproved(
                new SagaEventController.PaymentApprovedCommand(
                        uuid(root, "messageId"),
                        uuid(root, "correlationId"),
                        uuid(root, "sagaId"),
                        uuid(root, "serviceOrderId"),
                        uuid(payload, "paymentId"),
                        requiredText(amount, "amount"),
                        requiredText(amount, "currency"),
                        requiredText(payload, "externalPaymentId"),
                        instant(root, "occurredAt")
                )
        );
    }

    private Object serviceOrderReadyForExecution(JsonNode root) {
        var payload = root.path("payload");
        return controller.serviceOrderReadyForExecution(
                new SagaEventController.ServiceOrderReadyForExecutionCommand(
                        uuid(root, "messageId"),
                        uuid(root, "correlationId"),
                        uuid(root, "causationId"),
                        uuid(root, "sagaId"),
                        uuid(root, "serviceOrderId"),
                        uuid(payload, "paymentId"),
                        instant(root, "occurredAt")
                )
        );
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

    private UUID uuid(JsonNode node, String field) {
        return UUID.fromString(requiredText(node, field));
    }

    private Instant instant(JsonNode node, String field) {
        String value = requiredText(node, field);
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            return epoch(new BigDecimal(value));
        }
    }

    private Instant epoch(BigDecimal value) {
        if (value.abs().compareTo(new BigDecimal("100000000000")) >= 0) {
            return Instant.ofEpochMilli(value.longValueExact());
        }
        BigDecimal[] parts = value.divideAndRemainder(BigDecimal.ONE);
        return Instant.ofEpochSecond(
                parts[0].longValueExact(),
                parts[1].movePointRight(9).longValue()
        );
    }
}
