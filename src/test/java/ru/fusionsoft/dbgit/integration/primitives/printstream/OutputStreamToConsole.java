package ru.fusionsoft.dbgit.integration.primitives.printstream;

import java.io.OutputStream;

public class OutputStreamToConsole extends OutputStream {
    @Override
    public final void write(int i) {
        System.out.write(i);
    }
}
