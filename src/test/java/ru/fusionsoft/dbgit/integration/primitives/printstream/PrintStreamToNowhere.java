package ru.fusionsoft.dbgit.integration.primitives.printstream;

import java.io.PrintStream;

public class PrintStreamToNowhere extends PrintStream {

    public PrintStreamToNowhere() {
        super(new OutputStreamToNowhere());
    }
}
