package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitCommit extends PathPatchRunningDbGit {
    public PathPatchDbGitCommit(CharSequence message, PrintStream printStream) {
        super(new ArgsExplicit("commit", "-m", message), printStream);
    }

    public PathPatchDbGitCommit(CharSequence message) {
        this(message, new DefaultPrintStream());
    }
}
