package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithFiles;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;

public class PathAfterDbGitLinkAndAdd extends PathAfterDbGitRun {
    public PathAfterDbGitLinkAndAdd(ArgsDbGitLink argsDbGitLink, CharSequence ignoreChars, PrintStream printStream, Path origin) {
        super(
            new ArgsExplicit("add", "\"*\"", "-v"),
            printStream,
            new PathWithFiles(
                new PathPatchCreatingFile(".dbgit/.dbignore", ignoreChars),
                new PathAfterDbGitRun(argsDbGitLink, printStream, origin)
            )
        );
    }

    public PathAfterDbGitLinkAndAdd(ArgsDbGitLink argsDbGitLink, CharSequence ignoreChars, Path origin) {
        this(argsDbGitLink, ignoreChars, new DefaultPrintStream(), origin);
    }
}
