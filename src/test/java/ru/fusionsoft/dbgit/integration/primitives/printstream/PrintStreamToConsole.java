package ru.fusionsoft.dbgit.integration.primitives.printstream;

import java.io.PrintStream;

public class PrintStreamToConsole extends PrintStream {
    public PrintStreamToConsole() {
        super(new OutputStreamToConsole());
    }
}
