package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchRunningGit extends PathPatchRunningExecutable {
    public PathPatchRunningGit(Args gitArgs, PrintStream printStream) {
        super(
            "git",
            gitArgs,
            printStream
        );
    }

    public PathPatchRunningGit(Args gitArgs) {
        this(gitArgs, new DefaultPrintStream());
    }

    public PathPatchRunningGit(CharSequence... gitArgs) {
        this(new ArgsExplicit(gitArgs));
    }
}

