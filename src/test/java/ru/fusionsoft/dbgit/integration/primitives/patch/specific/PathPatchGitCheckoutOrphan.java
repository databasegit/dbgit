package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningGit;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchGitCheckoutOrphan extends PatchSequential<Path> {
    public PathPatchGitCheckoutOrphan(String newBranchName, PrintStream printStream) {
        super(
            new PathPatchRunningGit(
                new ArgsExplicit("checkout", "--orphan", newBranchName),
                printStream
            ),
            new PathPatchRunningGit(
                new ArgsExplicit("rm", "-rf", "."),
                printStream
            )
        );
    }

    public PathPatchGitCheckoutOrphan(String newBranchName) {
        this(newBranchName, new DefaultPrintStream());
    }
}
