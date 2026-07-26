package br.com.pitflow.saga.core.entity;
import java.time.Instant;
import java.util.UUID;
public record PaymentSaga(UUID sagaId, UUID serviceOrderId, Money amount, SagaState state, Instant createdAt, long version) {
    public static PaymentSaga start(UUID sagaId, UUID serviceOrderId, Money amount, Instant now) {
        return new PaymentSaga(sagaId, serviceOrderId, amount, SagaState.PAYMENT_CREATION_PENDING, now, 1);
    }
}
