package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAddRemote;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitClone;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchAssertsNotMvnProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFilesWildcard;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchDbGitClonesRepo extends PatchSequential<Path> {
    public PathPatchDbGitClonesRepo(CharSequence repoUrl, ArgsDbGitAddRemote addRemoteArgs, PrintStream printStream) {
        super(
            new PathPatchAssertsNotMvnProjectRoot(),
            new PathPatchDeletingFilesWildcard(
                "*"
            ),
            new PathPatchRunningDbGit(
                new ArgsDbGitClone(repoUrl),
                printStream
            ),
            new PathPatchRunningDbGit(
                addRemoteArgs,
                printStream
            )
        );
    }
    public PathPatchDbGitClonesRepo(CharSequence repoUrl, PrintStream printStream) {
        super(
            new PathPatchRunningDbGit(
                new ArgsDbGitClone(repoUrl),
                printStream
            )
        );
    }
    public PathPatchDbGitClonesRepo(CharSequence repoUrl, ArgsDbGitAddRemote addRemoteArgs) {
        this(repoUrl, addRemoteArgs, new DefaultPrintStream());
    }
}
