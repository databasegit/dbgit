package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAddRemote;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathWithDbGitRepoCloned;

public class PathAfterDbGitDumpsDbSchemaToGit extends PathPatched {
    public PathAfterDbGitDumpsDbSchemaToGit(
        CharSequence commitMessage,
        CharSequence pushRemoteName,
        CharSequence gitUrl,
        ArgsDbGitLink dbLinkArgs,
        ArgsDbGitAddRemote addRemoteArgs,
        CharSequence ignoreChars,
        Patch<Path> checkoutPatch,
        PrintStream printStream,
        Path workingDirectory
    ) {
        super(
            new PatchSequential<>(
                checkoutPatch,
                new PathPatchDbGitLink(dbLinkArgs, printStream),
                new PathPatchCreatingFile(".dbgit/.dbignore", ignoreChars),
                new PathPatchRunningDbGit(new ArgsExplicit("add", "\"*\"", "-v"), printStream),
                new PathPatchRunningDbGit(new ArgsExplicit("commit", "-m", commitMessage), printStream),
                new PathPatchRunningDbGit(new ArgsExplicit("push", pushRemoteName),printStream)
            ),
            new PathWithDbGitRepoCloned(gitUrl, addRemoteArgs, printStream, workingDirectory)
        );
    }

}
