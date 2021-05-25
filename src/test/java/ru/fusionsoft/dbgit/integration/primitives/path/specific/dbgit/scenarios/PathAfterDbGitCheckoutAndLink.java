package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitCheckout;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;

public class PathAfterDbGitCheckoutAndLink extends PathAfterDbGitRun {
    public PathAfterDbGitCheckoutAndLink(
        ArgsDbGitCheckout checkoutArgs,
        ArgsDbGitLink linkArgs,
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

    public PathAfterDbGitCheckoutAndLink(
        ArgsDbGitCheckout checkoutArgs,
        ArgsDbGitLink linkArgs,
        Path origin
    ) {
        this(checkoutArgs, linkArgs, new DefaultPrintStream(), origin);
    }
}
