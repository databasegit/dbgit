package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsRunningCommand;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;
import ru.fusionsoft.dbgit.integration.primitives.path.PathOfBuiltDbGitExecutable;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class PathPatchRunningDbGitFrom extends PathPatchRunningProcessFrom {
    public PathPatchRunningDbGitFrom(Args args, Path executablePath, PrintStream printStream) {
        super(
            new ArgsWithAppend(
                new ArgsRunningCommand(executablePath),
                args
            ),
            printStream
        );
    }

    public PathPatchRunningDbGitFrom(Args args, Path executablePath) {
        this(
            args, 
            executablePath, 
            System.out
        );
    }
    
    public PathPatchRunningDbGitFrom(Args args, PrintStream printStream) {
        this(
            args, 
            new PathOfBuiltDbGitExecutable(new CurrentWorkingDirectory()), 
            printStream
        );
    }

    public PathPatchRunningDbGitFrom(Args args) {
        this(
            args, 
            System.out
        );
    }
    

}
