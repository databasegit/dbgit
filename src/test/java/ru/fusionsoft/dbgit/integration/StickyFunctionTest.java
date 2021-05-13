package ru.fusionsoft.dbgit.integration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.Function;
import ru.fusionsoft.dbgit.integration.primitives.StickyFunction;

import java.util.concurrent.atomic.AtomicInteger;

public class StickyFunctionTest {

    @Test
    public void returnsValue() throws Exception {
        Assertions.assertEquals(
            4,
            new StickyFunction<Integer, Integer>( (x) -> x+2 ).value(2)
        );
    }

    @Test
    public void returnsSameValue() throws Exception {
        final AtomicInteger accessCount = new AtomicInteger(0);
        final Function<Integer, Integer> sticky = new StickyFunction<>(
            (x) -> accessCount.incrementAndGet()
        );
        Assertions.assertEquals(
            sticky.value(0),
            sticky.value(0)
        );
    }
}
