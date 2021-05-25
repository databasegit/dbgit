package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;

public class ArgsDbGitReset extends ArgsWithPrepend {
    public ArgsDbGitReset(CharSequence nameOfResetMode) {
        super(new ArgsExplicit(nameOfResetMode), "reset");
    }
}
