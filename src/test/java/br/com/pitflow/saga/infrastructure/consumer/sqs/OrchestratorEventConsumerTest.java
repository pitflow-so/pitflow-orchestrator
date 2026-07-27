package br.com.pitflow.saga.infrastructure.consumer.sqs;

import br.com.pitflow.saga.controller.SagaEventController;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OrchestratorEventConsumerTest {
    private final SqsClient sqs = mock(SqsClient.class);
    private final SagaEventController controller =
            mock(SagaEventController.class);
    private OrchestratorEventConsumer consumer;

    @BeforeEach
    void setUp() {
        when(sqs.getQueueUrl(any(GetQueueUrlRequest.class)))
                .thenReturn(GetQueueUrlResponse.builder()
                        .queueUrl("queue-url")
                        .build());
        consumer = new OrchestratorEventConsumer(
                sqs,
                new ObjectMapper(),
                controller,
                "service-order-orchestrator-queue",
                1
        );
    }

    @Test
    void dispatchesPaymentLinkCreatedAndDeletesAfterSuccess() {
        when(controller.paymentLinkCreated(any()))
                .thenReturn(br.com.pitflow.saga.core.gateway
                        .SagaPaymentLinkGateway.HandleResult.UPDATED);
        var message = Message.builder()
                .messageId("sqs-link")
                .receiptHandle("receipt-link")
                .body("""
                        {
                          "schemaVersion": 1,
                          "messageId": "10000000-0000-0000-0000-000000000001",
                          "type": "PaymentLinkCreated",
                          "occurredAt": "2026-07-26T20:00:00Z",
                          "correlationId": "20000000-0000-0000-0000-000000000002",
                          "sagaId": "30000000-0000-0000-0000-000000000003",
                          "serviceOrderId": "40000000-0000-0000-0000-000000000004",
                          "payload": {
                            "paymentId": "50000000-0000-0000-0000-000000000005",
                            "preferenceId": "pref-1",
                            "checkoutUrl": "https://sandbox.mercadopago.com/checkout",
                            "expiresAt": "2026-07-27T20:00:00Z"
                          }
                        }
                        """)
                .build();

        consumer.process(message);

        verify(controller).paymentLinkCreated(any());
        verify(sqs).deleteMessage(argThat(
                (DeleteMessageRequest request) ->
                        "receipt-link".equals(request.receiptHandle())
        ));
    }

    @Test
    void dispatchesServiceOrderAwaitingPaymentConfirmation() {
        when(controller.serviceOrderAwaitingPayment(any()))
                .thenReturn(br.com.pitflow.saga.core.gateway
                        .SagaPaymentLinkGateway.HandleResult.UPDATED);
        var message = Message.builder()
                .messageId("sqs-confirmation")
                .receiptHandle("receipt-confirmation")
                .body("""
                        {
                          "schemaVersion": 1,
                          "messageId": "10000000-0000-0000-0000-000000000001",
                          "type": "ServiceOrderAwaitingPayment",
                          "occurredAt": "2026-07-27T20:00:00Z",
                          "correlationId": "20000000-0000-0000-0000-000000000002",
                          "causationId": "30000000-0000-0000-0000-000000000003",
                          "sagaId": "40000000-0000-0000-0000-000000000004",
                          "serviceOrderId": "50000000-0000-0000-0000-000000000005",
                          "payload": {
                            "paymentId": "60000000-0000-0000-0000-000000000006"
                          }
                        }
                        """)
                .build();

        consumer.process(message);

        verify(controller).serviceOrderAwaitingPayment(any());
        verify(sqs).deleteMessage(argThat(
                (DeleteMessageRequest request) ->
                        "receipt-confirmation".equals(
                                request.receiptHandle()
                        )
        ));
    }

    @Test
    void deletesMessageOnlyAfterSuccessfulIdempotentHandling() {
        when(controller.budgetApproved(any()))
                .thenReturn(SagaStartGateway.StartResult.CREATED);
        var message = Message.builder()
                .messageId("sqs-1")
                .receiptHandle("receipt-1")
                .body("""
                        {
                          "schemaVersion": 1,
                          "messageId": "10000000-0000-0000-0000-000000000001",
                          "type": "ServiceOrderBudgetApproved",
                          "occurredAt": "2026-07-26T20:00:00Z",
                          "correlationId": "20000000-0000-0000-0000-000000000002",
                          "sagaId": null,
                          "serviceOrderId": "30000000-0000-0000-0000-000000000003",
                          "payload": {
                            "amount": {"amount": "450.00", "currency": "BRL"}
                          }
                        }
                        """)
                .build();

        consumer.process(message);

        verify(controller).budgetApproved(any());
        verify(sqs).deleteMessage(argThat(
                (DeleteMessageRequest request) ->
                "receipt-1".equals(request.receiptHandle())
        ));
    }

    @Test
    void keepsInvalidMessageForSqsRetryAndDlq() {
        var message = Message.builder()
                .messageId("sqs-invalid")
                .receiptHandle("receipt-invalid")
                .body("{\"schemaVersion\":2}")
                .build();

        consumer.process(message);

        verifyNoInteractions(controller);
        verify(sqs, never()).deleteMessage(any(DeleteMessageRequest.class));
    }
}
