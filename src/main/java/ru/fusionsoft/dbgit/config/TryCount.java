package ru.fusionsoft.dbgit.config;

import java.util.function.Supplier;
import org.cactoos.scalar.Unchecked;
import ru.fusionsoft.dbgit.core.DBGitConfig;

public class TryCount implements Supplier<Integer> {

    @Override
    public final Integer get() {
        return new Unchecked<Integer>(() -> {
            return DBGitConfig.getInstance().getInteger(
                "core",
                "TRY_COUNT",
                DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000)
            );
        }).value();
    }
}
