package ru.fusionsoft.dbgit.integration.primitives.path;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsRunningCommand;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;

public class PathAfterGitRun extends PathAfterProcessRun {
    public PathAfterGitRun(Args gitArgs, PrintStream printStream, Path origin) {
        super(
            new ArgsWithAppend(new ArgsRunningCommand("git"), gitArgs),
            printStream,
            origin
        );
    }
    public PathAfterGitRun(Args gitArgs, Path origin) {
        this(gitArgs, new DefaultPrintStream(), origin);
    }
}
