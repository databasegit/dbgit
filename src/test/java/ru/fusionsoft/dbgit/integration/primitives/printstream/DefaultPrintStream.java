package ru.fusionsoft.dbgit.integration.primitives.printstream;

import java.io.PrintStream;

public class DefaultPrintStream extends PrintStream {
    public DefaultPrintStream() {
        super(new PrintStreamToConsole());
    }
}
