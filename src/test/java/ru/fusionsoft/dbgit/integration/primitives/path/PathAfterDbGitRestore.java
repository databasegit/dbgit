package ru.fusionsoft.dbgit.integration.primitives.path;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.NullPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsCheckoutNodb;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class PathAfterDbGitRestore extends PathAfterDbGitRun {

    public PathAfterDbGitRestore(ArgsCheckoutNodb checkoutArgs, ArgsLink linkArgs, Path dbGitExecutablePath, PrintStream printStream, Path dbGitRepo) {
        super(
            new ArgsExplicit("restore", "-r"),
            dbGitExecutablePath,
            printStream,
            new PathWithDbGitCheckoutAndLink(
                checkoutArgs,
                linkArgs,
                printStream,
                dbGitRepo
            )
        );
    }

    public PathAfterDbGitRestore(ArgsCheckoutNodb checkoutArgs, ArgsLink linkArgs, Path dbGitExecutablePath, Path dbGitRepo) {
        this(checkoutArgs, linkArgs, dbGitExecutablePath, new NullPrintStream(), dbGitRepo);
    }
    

    public PathAfterDbGitRestore(ArgsCheckoutNodb checkoutArgs, ArgsLink linkArgs, Path dbGitRepo) {
        this(
            checkoutArgs, 
            linkArgs, 
            new CurrentWorkingDirectory(), 
            dbGitRepo
        );
    }

}
