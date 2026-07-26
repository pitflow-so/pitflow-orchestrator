package br.com.pitflow.saga.core.usecase;
import br.com.pitflow.saga.core.entity.PaymentSaga;
import br.com.pitflow.saga.core.event.BudgetApprovedEvent;
import br.com.pitflow.saga.core.event.CreatePaymentCommand;
import br.com.pitflow.saga.core.gateway.SagaStartGateway;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;
public class StartPaymentSagaImp implements StartPaymentSaga {
    private final SagaStartGateway gateway; private final Clock clock; private final Supplier<UUID> ids;
    public StartPaymentSagaImp(SagaStartGateway gateway, Clock clock, Supplier<UUID> ids) {
        this.gateway = gateway; this.clock = clock; this.ids = ids;
    }
    public SagaStartGateway.StartResult execute(BudgetApprovedEvent event) {
        var now = clock.instant(); var sagaId = ids.get();
        var saga = PaymentSaga.start(sagaId, event.serviceOrderId(), event.amount(), now);
        var command = new CreatePaymentCommand(ids.get(), event.correlationId(), event.messageId(), sagaId,
                event.serviceOrderId(), event.amount(), "Pagamento da ordem de serviço " + event.serviceOrderId(), sagaId, now);
        return gateway.startAtomically(event, saga, command);
    }
}
