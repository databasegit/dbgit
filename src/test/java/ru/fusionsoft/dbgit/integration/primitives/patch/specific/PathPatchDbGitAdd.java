package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAdd;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningDbGit;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchDbGitAdd extends PathPatchRunningDbGit {
    public PathPatchDbGitAdd(CharSequence mask, PrintStream printStream) {
        super(new ArgsDbGitAdd(mask), printStream);
    }    
    public PathPatchDbGitAdd(PrintStream printStream) {
        super(new ArgsDbGitAdd(), printStream);
    }
    public PathPatchDbGitAdd(CharSequence mask) {
        this(mask, new DefaultPrintStream());
    }
    public PathPatchDbGitAdd() {
        this(new DefaultPrintStream());
    }
}
