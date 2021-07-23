package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;

public class PathPatchDbGitLinkAndAdd extends PatchSequential<Path> {
    public PathPatchDbGitLinkAndAdd(
        ArgsDbGitLink argsDbGitLink, 
        CharSequence ignoreChars, 
        PrintStream printStream
    ) {
        super(
            new PathPatchDbGitLink(argsDbGitLink, printStream),
            new PathPatchCreatingFile(".dbgit/.dbignore", ignoreChars, printStream),
            new PathPatchDbGitAdd(printStream)
        );
    }
}
