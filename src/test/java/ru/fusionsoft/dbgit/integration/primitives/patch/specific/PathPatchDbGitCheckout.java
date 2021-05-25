package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitCheckout;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitCheckout extends PathPatchRunningDbGit {
    public PathPatchDbGitCheckout(ArgsDbGitCheckout args, PrintStream printStream) {
        super(args, printStream);
    }
    public PathPatchDbGitCheckout(ArgsDbGitCheckout args) {
        this(args, new DefaultPrintStream());
    }
    public PathPatchDbGitCheckout(CharSequence... args) {
        this(new ArgsDbGitCheckout(args));
    }
}
