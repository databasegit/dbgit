package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.NullPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequental;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;

public class PathPatchCloningGitRepo extends PatchSequental<Path> {

    public PathPatchCloningGitRepo(final String repoUrl, final String branchName, PrintStream printStream) {
        super(
            new PathPatchRunningProcessFrom(
                new ArgsExplicit(
                    System.getenv("ComSpec"),
                    "/C",
                    "git",
                    "clone",
                    repoUrl,
                    "."
                ),
                printStream
            ),
            new PathPatchRunningProcessFrom(
                new ArgsExplicit(
                    System.getenv("ComSpec"),
                    "/C",
                    "git",
                    "reset",
                    "--hard",
                    branchName
                ),
                printStream
            )
            
        );
    }

    public PathPatchCloningGitRepo(final String repoUrl, final String branchName) {
        this(repoUrl, branchName, new NullPrintStream());
    }
}
