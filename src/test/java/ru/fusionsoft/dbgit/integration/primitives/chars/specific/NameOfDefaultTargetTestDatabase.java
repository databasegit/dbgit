package ru.fusionsoft.dbgit.integration.primitives.chars.specific;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class NameOfDefaultTargetTestDatabase extends CharSequenceEnvelope {
    public NameOfDefaultTargetTestDatabase() {
        super(()->"test#databasegit");
    }
}
