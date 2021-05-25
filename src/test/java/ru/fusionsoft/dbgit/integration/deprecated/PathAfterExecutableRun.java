package ru.fusionsoft.dbgit.integration.deprecated;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterProcessRun;
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
            new PathAfterProcessRun(
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
            new DefaultPrintStream(),
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
            new DefaultPrintStream(),
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
