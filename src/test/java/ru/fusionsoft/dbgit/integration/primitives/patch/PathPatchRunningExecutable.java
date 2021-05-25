package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsRunningCommand;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;

public class PathPatchRunningExecutable extends PathPatchRunningProcessFrom {

    public PathPatchRunningExecutable(CharSequence commandName, Args commandArgs, PrintStream printStream) {
        super(
            new ArgsWithAppend(
                new ArgsRunningCommand(commandName),
                commandArgs
            ), 
            printStream
        );
    }
    
}
