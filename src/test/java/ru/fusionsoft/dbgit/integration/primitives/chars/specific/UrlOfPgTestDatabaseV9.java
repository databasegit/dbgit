package ru.fusionsoft.dbgit.integration.primitives.chars.specific;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class UrlOfPgTestDatabaseV9 extends CharSequenceEnvelope {
    public UrlOfPgTestDatabaseV9() {
        super(()-> "jdbc:postgresql://135.181.94.98:31007");
    }
}
