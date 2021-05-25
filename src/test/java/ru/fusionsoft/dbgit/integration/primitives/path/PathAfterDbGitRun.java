package ru.fusionsoft.dbgit.integration.primitives.path;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathOfBuiltDbGitExecutable;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsRunningCommand;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class PathAfterDbGitRun extends PathAfterProcessRun {
    public PathAfterDbGitRun(Args args, Path executablePath, PrintStream printStream, Path workingDirectory) {
        super(
            new ArgsWithAppend(
                new ArgsRunningCommand(executablePath),
                args
            ),
            printStream,
            workingDirectory
        );
    }

    public PathAfterDbGitRun(Args args, PrintStream printStream, Path workingDirectory) {
        this(
            args,
            new PathOfBuiltDbGitExecutable(new CurrentWorkingDirectory()),
            printStream,
            workingDirectory
        );
    }

    public PathAfterDbGitRun(Args args, Path executablePath, Path workingDirectory) {
        this(
            args,
            executablePath,
            new DefaultPrintStream(),
            workingDirectory
        );
    }

    public PathAfterDbGitRun(Args args, Path workingDirectory) {
        this(
            args,
            new PathOfBuiltDbGitExecutable(new CurrentWorkingDirectory()),
            workingDirectory
        );
    }
}
