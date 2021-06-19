package ru.fusionsoft.dbgit.integration.primitives.chars.specific;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class UrlOfPgTestDatabaseV11 extends CharSequenceEnvelope {
    public UrlOfPgTestDatabaseV11() {
        super(()-> "jdbc:postgresql://135.181.94.98:31107");
    }
}
