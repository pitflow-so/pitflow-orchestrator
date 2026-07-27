package br.com.pitflow.saga.core.usecase;

import br.com.pitflow.saga.core.event.MarkServiceOrderReadyForExecutionCommand;
import br.com.pitflow.saga.core.event.PaymentApprovedEvent;
import br.com.pitflow.saga.core.gateway.SagaPaymentApprovalGateway;

import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

public final class HandlePaymentApprovedImp implements HandlePaymentApproved {
    private final SagaPaymentApprovalGateway gateway;
    private final Clock clock;
    private final Supplier<UUID> ids;

    public HandlePaymentApprovedImp(
            SagaPaymentApprovalGateway gateway,
            Clock clock,
            Supplier<UUID> ids
    ) {
        this.gateway = gateway;
        this.clock = clock;
        this.ids = ids;
    }

    @Override
    public SagaPaymentApprovalGateway.HandleResult execute(
            PaymentApprovedEvent event
    ) {
        validate(event);
        var command = new MarkServiceOrderReadyForExecutionCommand(
                ids.get(),
                event.correlationId(),
                event.messageId(),
                event.sagaId(),
                event.serviceOrderId(),
                event.paymentId(),
                event.externalPaymentId(),
                clock.instant()
        );
        return gateway.handleAtomically(event, command);
    }

    private static void validate(PaymentApprovedEvent event) {
        if (event == null
                || event.messageId() == null
                || event.correlationId() == null
                || event.sagaId() == null
                || event.serviceOrderId() == null
                || event.paymentId() == null
                || event.approvedAmount() == null
                || event.externalPaymentId() == null
                || event.externalPaymentId().isBlank()
                || event.occurredAt() == null) {
            throw new IllegalArgumentException("Invalid PaymentApproved event");
        }
    }
}
