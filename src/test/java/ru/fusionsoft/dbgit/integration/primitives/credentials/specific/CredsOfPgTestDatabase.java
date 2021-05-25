package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.credentials.SimpleCredentials;

public class CredsOfPgTestDatabase extends CredentialsEnvelope {
    public CredsOfPgTestDatabase() {
        super(() -> {
            try {
                final Credentials creds = new CredsFromPgMvnDProps();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            } catch (Throwable e) {
                final Credentials creds = new CredsFromPgSecretFile();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            }
        });
    }
}
