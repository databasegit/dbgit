package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitRestore;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitRestore extends PathPatchRunningDbGit {
    public PathPatchDbGitRestore(ArgsDbGitRestore restoreArgs, PrintStream printStream) {
        super(restoreArgs, printStream);
    }

    public PathPatchDbGitRestore(ArgsDbGitRestore restoreArgs) {
        this(restoreArgs, new DefaultPrintStream());
    }
    
    public PathPatchDbGitRestore(CharSequence... restoreCommandArgs) {
        this(new ArgsDbGitRestore(restoreCommandArgs), new DefaultPrintStream());
    }
}
