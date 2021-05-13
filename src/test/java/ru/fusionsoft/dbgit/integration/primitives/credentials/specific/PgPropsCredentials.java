package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.FromPropertiesCredentials;

public class PgPropsCredentials extends FromPropertiesCredentials {
    public PgPropsCredentials() {
        super("pgUser", "pgPass");
    }
}
