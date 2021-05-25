package ru.fusionsoft.dbgit.integration.primitives.args.specific;

public class ArgsDbGitLinkPgAuto extends ArgsDbGitLink {
    public ArgsDbGitLinkPgAuto(CharSequence databaseName) {
        super(()->new ArgsDbGitLinkPgRemote(databaseName));
    }
}
