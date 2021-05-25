package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithAppend;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;

public class ArgsDbGitCheckout extends ArgsWithPrepend {

    public ArgsDbGitCheckout(CharSequence... args) {
        super(
            new ArgsExplicit(args),
            "checkout"
        );
    }
  
}
