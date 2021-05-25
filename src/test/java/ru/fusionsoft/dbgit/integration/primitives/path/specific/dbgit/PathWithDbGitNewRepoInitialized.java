package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithoutFiles;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathWithDbGitNewRepoInitialized extends PathAfterDbGitRun {
    public PathWithDbGitNewRepoInitialized(PrintStream printStream, Path workingDirectory) {
        super(
            new ArgsExplicit("init"),
            printStream,

            new PathWithoutFiles(
                "*",
                new PathNotProjectRoot(
                    workingDirectory
                )
            )
        );
    }

    public PathWithDbGitNewRepoInitialized(Path workingDirectory) {
        this(new DefaultPrintStream(), workingDirectory);
    }
}
