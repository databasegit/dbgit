package ru.fusionsoft.dbgit.integration.primitives.path;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.NullPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsCheckoutNodb;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;

public class PathWithDbGitCheckoutAndLink extends PathAfterDbGitRun {
    public PathWithDbGitCheckoutAndLink(
        ArgsCheckoutNodb checkoutArgs,
        ArgsLink linkArgs,
        PrintStream printStream,
        Path origin
    ) {
        super(
            linkArgs,
            printStream,
            new PathAfterDbGitRun(
                checkoutArgs, 
                origin
            )
        );
    }

    public PathWithDbGitCheckoutAndLink(
        ArgsCheckoutNodb checkoutArgs,
        ArgsLink linkArgs,
        Path origin
    ) {
        this(checkoutArgs, linkArgs, new NullPrintStream(), origin);
    }
}
