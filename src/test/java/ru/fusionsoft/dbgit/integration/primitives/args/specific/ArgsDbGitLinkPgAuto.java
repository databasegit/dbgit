package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.chars.specific.UrlOfPgTestDatabaseV11;

public class ArgsDbGitLinkPgAuto extends ArgsDbGitLink {
    public ArgsDbGitLinkPgAuto(CharSequence databaseName) {
        super(()->new ArgsDbGitLinkPgRemote(new UrlOfPgTestDatabaseV11(), databaseName));
    }
}
