package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsRunningCommand;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathOfBuiltDbGitExecutable;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchRunningDbGit extends PathPatchRunningProcessFrom {
    public PathPatchRunningDbGit(Args args, Path executablePath, PrintStream printStream) {
        super(
            new ArgsWithAppend(
                new ArgsRunningCommand(executablePath),
                args
            ),
            printStream
        );
    }

    public PathPatchRunningDbGit(Args args, Path executablePath) {
        this(
            args,
            executablePath,
            System.out
        );
    }

    public PathPatchRunningDbGit(Args args, PrintStream printStream) {
        this(
            args,
            new PathOfBuiltDbGitExecutable(new CurrentWorkingDirectory()),
            printStream
        );
    }

    public PathPatchRunningDbGit(Args args) {
        this(args, new DefaultPrintStream());
    }
}
