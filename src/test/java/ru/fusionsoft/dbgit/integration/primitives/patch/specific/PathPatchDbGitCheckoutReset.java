package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitCheckout;

public class PathPatchDbGitCheckoutReset extends PatchSequential<Path> {
    public PathPatchDbGitCheckoutReset(ArgsDbGitCheckout checkoutArgs, PrintStream printStream) {
        super(
            new PathPatchDbGitCheckout(checkoutArgs, printStream), 
            new PathPatchDbGitReset("-hard")
        );
    }

    public PathPatchDbGitCheckoutReset(ArgsDbGitCheckout checkoutArgs) {
        this(checkoutArgs, new DefaultPrintStream());
    }

    public PathPatchDbGitCheckoutReset(CharSequence... checkoutParams) {
        this(new ArgsDbGitCheckout(checkoutParams));
    }

}
