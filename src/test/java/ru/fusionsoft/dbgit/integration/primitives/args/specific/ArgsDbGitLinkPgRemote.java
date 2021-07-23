package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfPgTestDatabase;

public class ArgsDbGitLinkPgRemote extends ArgsDbGitLink {
    public ArgsDbGitLinkPgRemote(CharSequence dbmsUrl, CharSequence database, CharSequence username, CharSequence password) {
        super(
            dbmsUrl,
            database,
            username,
            password
        );
    }

    public ArgsDbGitLinkPgRemote(CharSequence dbmsUrl, CharSequence database, Credentials credentials) {
        this(dbmsUrl, database, credentials.username(), credentials.password());
    }

    public ArgsDbGitLinkPgRemote(CharSequence dbmsUrl, CharSequence database) {
        this(
            dbmsUrl,
            database,
            new CredsOfPgTestDatabase()
        );
    }
}
