package ru.fusionsoft.dbgit.integration.primitives.args;

import org.apache.commons.lang3.ArrayUtils;
import ru.fusionsoft.dbgit.integration.primitives.Args;

public class ArgsWithPrepend implements Args {
    private final Args origin;
    private final Args prepend;

    public ArgsWithPrepend(Args origin, Args prepend) {
        this.origin = origin;
        this.prepend = prepend;
    }
    public ArgsWithPrepend(Args origin, CharSequence... prepend) {
        this.origin = origin;
        this.prepend = new ArgsExplicit(prepend);
    }

    @Override
    public final CharSequence[] values() {
        return ArrayUtils.addAll(
            prepend.values(),
            origin.values()
        );
    }
}
