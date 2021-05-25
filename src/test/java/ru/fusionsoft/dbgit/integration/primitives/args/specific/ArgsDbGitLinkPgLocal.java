package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfPgTestDatabase;

public class ArgsDbGitLinkPgLocal extends ArgsDbGitLink {
    public ArgsDbGitLinkPgLocal(CharSequence database, CharSequence user, CharSequence pass) {
        super(
            "jdbc:postgresql://localhost",
            database,
            user,
            pass
        );

    }
    public ArgsDbGitLinkPgLocal(CharSequence database, Credentials credentials) {
        this(database, credentials.username(), credentials.password());
    }
    public ArgsDbGitLinkPgLocal(CharSequence database) {
        this(database, new CredsOfPgTestDatabase());
    }
}
