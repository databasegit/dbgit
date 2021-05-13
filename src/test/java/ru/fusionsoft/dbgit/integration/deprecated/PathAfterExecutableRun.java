package ru.fusionsoft.dbgit.integration.deprecated;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.NullPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterCommandRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class PathAfterExecutableRun extends PathEnvelope {
    public PathAfterExecutableRun(
        Args commandInterface,
        CharSequence executableName,
        Args command,
        PrintStream printStream,
        Path origin
    ) {
        super(
            () ->
            new PathAfterCommandRun(
                new ArgsWithPrepend(
                    new ArgsWithPrepend(
                        command,
                        executableName
                    ),
                    commandInterface
                ),
                printStream,
                origin
            )
        );
    }

    public PathAfterExecutableRun(
        Args commandInterface,
        CharSequence executableName,
        Args executableArgs,
        Path workingDirectory
    ) {
        this(
            commandInterface,
            executableName,
            executableArgs,
            new NullPrintStream(),
            workingDirectory
        );
    }

    public PathAfterExecutableRun(
        CharSequence executableName,
        Args args,
        PrintStream printStream,
        Path workingDirectory
    ) {
        this(
            new ArgsExplicit(System.getenv("ComSpec"), "/C"),
            executableName,
            args,
            printStream,
            workingDirectory
        );
    }

    public PathAfterExecutableRun(
        CharSequence executableName, 
        Args args, 
        Path workingDirectory
    ) {
        this(
            executableName,
            args,
            new NullPrintStream(),
            workingDirectory
        );
    }

    public PathAfterExecutableRun(Path executablePath, Args args, Path workingDirectory) {
        this(
            new CharsOf<>(Path::toString, executablePath),
            args,
            workingDirectory
        );
    }

}
