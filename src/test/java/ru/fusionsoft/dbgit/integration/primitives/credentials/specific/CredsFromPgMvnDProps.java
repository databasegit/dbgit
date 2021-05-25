package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsFromProperties;

public class CredsFromPgMvnDProps extends CredentialsFromProperties {
    public CredsFromPgMvnDProps() {
        super("pgUser", "pgPass");
    }
}
