package ru.fusionsoft.dbgit.integration.primitives.args;

import org.apache.commons.lang3.ArrayUtils;
import ru.fusionsoft.dbgit.integration.primitives.Args;

public class ArgsWithAppend implements Args {
    private final Args origin;
    private final Args append;

    public ArgsWithAppend(Args origin, Args append) {
        this.origin = origin;
        this.append = append;
    }
    
    public ArgsWithAppend(Args origin, String... append) {
        this.origin = origin;
        this.append = new ArgsExplicit(append.clone());
    }

    @Override
    public final CharSequence[] values() {
        return ArrayUtils.addAll(
            origin.values(),
            append.values()
        );
    }
}
