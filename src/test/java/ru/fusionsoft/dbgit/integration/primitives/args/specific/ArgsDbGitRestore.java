package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;

public class ArgsDbGitRestore extends ArgsWithPrepend {
    public ArgsDbGitRestore(CharSequence... restoreCommandArgs) {
        super(new ArgsExplicit(restoreCommandArgs), "restore");
    }
}
