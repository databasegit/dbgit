package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;

public class PathPatchCloningGitRepo extends PatchSequential<Path> {

    public PathPatchCloningGitRepo(final String repoUrl, final String branchName, PrintStream printStream) {
        super(
            new PathPatchRunningExecutable(
                "git",
                new ArgsExplicit(
                    "clone",
                    repoUrl,
                    "."
                ),
                printStream
            ),
            new PathPatchRunningExecutable(
                "git",
                new ArgsExplicit(
                    "reset",
                    "--hard",
                    branchName
                ),
                printStream
            )
            
        );
    }

    public PathPatchCloningGitRepo(final String repoUrl, final String branchName) {
        this(repoUrl, branchName, new DefaultPrintStream());
    }
}
