package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitLinkAndAdd;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;

public class PathAfterDbGitLinkAndAdd extends PathPatched {
    public PathAfterDbGitLinkAndAdd(ArgsDbGitLink argsDbGitLink, CharSequence ignoreChars, PrintStream printStream, Path workingDirectory) {
        super(
            new PathPatchDbGitLinkAndAdd(argsDbGitLink, ignoreChars, printStream),
            workingDirectory
        );
    }

    public PathAfterDbGitLinkAndAdd(ArgsDbGitLink argsDbGitLink, CharSequence ignoreChars, Path origin) {
        this(argsDbGitLink, ignoreChars, new DefaultPrintStream(), origin);
    }
}
