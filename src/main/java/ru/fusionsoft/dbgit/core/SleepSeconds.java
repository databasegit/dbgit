package ru.fusionsoft.dbgit.core;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SleepSeconds implements Consumer<Integer> {
    @Override
    public final void accept(Integer seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
