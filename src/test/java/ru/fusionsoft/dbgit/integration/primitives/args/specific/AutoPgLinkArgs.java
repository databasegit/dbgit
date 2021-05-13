package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;

public class AutoPgLinkArgs extends ArgsLink {
    public AutoPgLinkArgs(String database) {
        super(()->new LocalPgLinkArgs(database));
    }
    
}
