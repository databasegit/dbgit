package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitReset;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;

public class PathPatchDbGitReset extends PathPatchRunningDbGit {
    public PathPatchDbGitReset(CharSequence resetMode, PrintStream printStream) {
        super(new ArgsDbGitReset(resetMode), printStream);
    }

    public PathPatchDbGitReset(CharSequence resetMode) {
        this(resetMode, new DefaultPrintStream());
    }
    
    public PathPatchDbGitReset() {
        this("hard", new DefaultPrintStream());
    }
}
