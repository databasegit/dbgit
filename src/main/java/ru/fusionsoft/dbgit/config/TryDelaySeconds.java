package ru.fusionsoft.dbgit.config;

import java.util.function.Supplier;
import org.cactoos.scalar.Unchecked;
import ru.fusionsoft.dbgit.core.DBGitConfig;

public class TryDelaySeconds implements Supplier<Integer> {
    @Override
    public final Integer get() {
        return new Unchecked<>(
            () -> DBGitConfig.getInstance().getInteger(
                "core",
                "TRY_DELAY",
                DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000)
            )
        ).value();
    }
}
