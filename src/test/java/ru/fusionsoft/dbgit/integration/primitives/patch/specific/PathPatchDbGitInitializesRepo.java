package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitInit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchAssertsNotMvnProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFilesWildcard;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchDbGitInitializesRepo extends PatchSequential<Path> {
    public PathPatchDbGitInitializesRepo(ArgsDbGitLink linkArgs, PrintStream printStream) {
        super(
            new PathPatchAssertsNotMvnProjectRoot(),
            new PathPatchDeletingFilesWildcard("*"),
            new PathPatchRunningDbGit(new ArgsDbGitInit(), printStream),
            new PathPatchRunningDbGit(linkArgs, printStream)
        );
    }

    public PathPatchDbGitInitializesRepo(PrintStream printStream) {
        super(
            new PathPatchAssertsNotMvnProjectRoot(),
            new PathPatchDeletingFilesWildcard("*"),
            new PathPatchRunningDbGit(new ArgsDbGitInit(), printStream)
        );
    }

    public PathPatchDbGitInitializesRepo(ArgsDbGitLink linkArgs) {
        this(linkArgs, new DefaultPrintStream());
    }
    
    public PathPatchDbGitInitializesRepo() {
        this(new DefaultPrintStream());
    }
}
