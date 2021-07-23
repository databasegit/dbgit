package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitRestore;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit.CharsDbGitConfigBackupEnabled;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitRestoreFromDbToDb extends PatchSequential<Path> {
    public PathPatchDbGitRestoreFromDbToDb(
        ArgsDbGitLink sourceDbLinkArgs,
        ArgsDbGitLink targetDbLinkArgs,
        CharSequence dbIgnoreChars,
        ArgsDbGitRestore restoreArgs,
        PrintStream printStream
    ) {
        super(
            new PathPatchRunningDbGit(new ArgsExplicit("rm", "\"*\"", "-idx", "-v"), printStream),
            new PathPatchDbGitLink(sourceDbLinkArgs, printStream),
            new PathPatchCreatingFile(".dbgit/.dbignore", dbIgnoreChars),
            new PathPatchDbGitAdd(printStream),
            new PathPatchDbGitLink(targetDbLinkArgs, printStream),
            new PathPatchCreatingFile(".dbgit/dbgitconfig", new CharsDbGitConfigBackupEnabled()),
            new PathPatchDbGitRestore(restoreArgs, printStream)
        );
    }
}
