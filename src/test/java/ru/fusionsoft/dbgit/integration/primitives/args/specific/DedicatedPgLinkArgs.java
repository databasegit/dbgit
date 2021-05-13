package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.PgCredentials;

public class DedicatedPgLinkArgs extends ArgsLink {
    public DedicatedPgLinkArgs(String database, String username, String password) {
        super(
            "jdbc:postgresql://135.181.94.98:31007/",
            database,
            username,
            password
        );
    }

    public DedicatedPgLinkArgs(String database, Credentials credentials) {
        this(database, credentials.username(), credentials.password());
    }

    public DedicatedPgLinkArgs(String database) {
        this(
            database,
            new PgCredentials()
        );
    }
}
