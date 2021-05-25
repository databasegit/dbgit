package ru.fusionsoft.dbgit.integration.primitives.printstream;

import java.io.OutputStream;

public class OutputStreamToNowhere extends OutputStream {
    @Override
    public void write(int b) {
    }
}
