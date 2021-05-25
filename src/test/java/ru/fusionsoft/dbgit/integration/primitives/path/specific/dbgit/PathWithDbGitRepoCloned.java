package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAddRemote;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitClonesRepo;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathWithDbGitRepoCloned extends PathPatched {
    public PathWithDbGitRepoCloned(CharSequence repoUrl, ArgsDbGitAddRemote addRemoteArgs, PrintStream printStream, Path origin) {
        super(
            new PathPatchDbGitClonesRepo(repoUrl, addRemoteArgs, printStream),
            origin
        );
    }
    public PathWithDbGitRepoCloned(CharSequence repoUrl, PrintStream printStream, Path origin) {
        super(
            new PathPatchDbGitClonesRepo(repoUrl, printStream),
            origin
        );
    }
    public PathWithDbGitRepoCloned(CharSequence repoUrl, ArgsDbGitAddRemote addRemoteArgs, Path origin) {
        this(repoUrl, addRemoteArgs, new DefaultPrintStream(), origin);
    }  
    public PathWithDbGitRepoCloned(CharSequence repoUrl, Path origin) {
        this(repoUrl, new DefaultPrintStream(), origin);
    }

}
