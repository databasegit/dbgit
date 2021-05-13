package ru.fusionsoft.dbgit.integration.primitives.args;

import ru.fusionsoft.dbgit.integration.primitives.Args;

public class ArgsExplicit implements Args {
    private final CharSequence[] args;
    public ArgsExplicit(CharSequence... values) {
        this.args = values.clone();
    }
    public final CharSequence[] values() {
        return this.args;
    }
}
