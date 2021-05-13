package ru.fusionsoft.dbgit.integration.primitives.path;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningProcessFrom;

public class PathAfterCommandRun extends PathPatched {
    
//    public PathAfterCommandRun(Args processRunCommandLine, Path origin) {
//        super(
//            origin, 
//            new PathPatchRunningProcessFrom(processRunCommandLine)
//        );
//    }    
    
    public PathAfterCommandRun(Args processRunCommandLine, PrintStream printStream, Path origin) {
        super(
            origin, 
            new PathPatchRunningProcessFrom(processRunCommandLine, printStream)
        );
    }
}
