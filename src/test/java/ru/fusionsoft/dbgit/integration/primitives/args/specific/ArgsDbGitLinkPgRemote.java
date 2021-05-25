package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfPgTestDatabase;

public class ArgsDbGitLinkPgRemote extends ArgsDbGitLink {
    public ArgsDbGitLinkPgRemote(CharSequence database, CharSequence username, CharSequence password) {
        super(
            "jdbc:postgresql://135.181.94.98:31007",
            database,
            username,
            password
        );
    }

    public ArgsDbGitLinkPgRemote(CharSequence database, Credentials credentials) {
        this(database, credentials.username(), credentials.password());
    }

    public ArgsDbGitLinkPgRemote(CharSequence database) {
        this(
            database,
            new CredsOfPgTestDatabase()
        );
    }
}
