package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.PgCredentials;

public class LocalPgLinkArgs extends ArgsLink {
    public LocalPgLinkArgs(String database, String user, String pass) {
        super(
            "jdbc:postgresql://localhost",
            database,
            user,
            pass
        );

    }
    public LocalPgLinkArgs(String database, Credentials credentials) {
        this(database, credentials.username(), credentials.password());
    }
    public LocalPgLinkArgs(String database) {
        this(database, new PgCredentials());
    }
}
