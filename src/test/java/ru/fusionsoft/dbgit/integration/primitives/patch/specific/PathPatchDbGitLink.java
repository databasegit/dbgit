package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitLink extends PathPatchRunningDbGit {

    public PathPatchDbGitLink(ArgsDbGitLink args, PrintStream printStream) {
        super(args, printStream);
    }

    public PathPatchDbGitLink(ArgsDbGitLink args) {
        this(args, new DefaultPrintStream());
    }
}
