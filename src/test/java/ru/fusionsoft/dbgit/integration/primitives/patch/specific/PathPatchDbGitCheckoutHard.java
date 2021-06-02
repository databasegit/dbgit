package ru.fusionsoft.dbgit.integration.primitives.patch.specific;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitCheckout;

public class PathPatchDbGitCheckoutHard extends PatchSequential<Path> {
    public PathPatchDbGitCheckoutHard(ArgsDbGitCheckout argsDbGitCheckout, PrintStream printStream) {
        super(
            new PathPatchDbGitReset("-hard"),
            new PathPatchDbGitCheckout(argsDbGitCheckout, printStream)
        );
    }

    public PathPatchDbGitCheckoutHard(CharSequence... checkoutArgs) {
        super(
            new PathPatchDbGitReset("-hard"),
            new PathPatchDbGitCheckout(new ArgsDbGitCheckout(checkoutArgs))
        );
    }
}
