package br.com.pitflow.saga.core.entity;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class MoneyTest {
    @Test
    void preservesExactDecimalValue() {
        var money = new Money(new BigDecimal("450.00"), "BRL");
        assertEquals("450.00", money.serializedAmount());
    }

    @Test
    void rejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new Money(BigDecimal.ZERO, "BRL"));
        assertThrows(IllegalArgumentException.class,
                () -> new Money(new BigDecimal("1.001"), "BRL"));
        assertThrows(IllegalArgumentException.class,
                () -> new Money(BigDecimal.ONE, "USD"));
    }
}
