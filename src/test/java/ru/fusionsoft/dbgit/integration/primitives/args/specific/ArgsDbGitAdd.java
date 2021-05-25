package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;

public class ArgsDbGitAdd extends ArgsWithPrepend {
    public ArgsDbGitAdd(CharSequence mask) {
        super(new ArgsExplicit(mask), "add", "-v");
    }
    public ArgsDbGitAdd() {
        this("\"*\"");
    }
}
