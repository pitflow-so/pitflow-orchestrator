package br.com.pitflow.saga.core.entity;
import java.math.BigDecimal;
import java.math.RoundingMode;
public record Money(BigDecimal amount, String currency) {
    public Money {
        if (amount == null || amount.signum() <= 0) throw new IllegalArgumentException("Amount must be positive");
        if (!"BRL".equals(currency)) throw new IllegalArgumentException("Only BRL is supported");
        try {
            amount = amount.setScale(2, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(
                    "Amount must have at most two decimal places",
                    exception
            );
        }
    }
    public String serializedAmount() { return amount.toPlainString(); }
}
