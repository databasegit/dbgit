package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public abstract class PathPatchWithPrintStream implements Patch<Path> {
    protected final PrintStream printStream;

    public PathPatchWithPrintStream(PrintStream printStream) {
        this.printStream = printStream;
    }
}
