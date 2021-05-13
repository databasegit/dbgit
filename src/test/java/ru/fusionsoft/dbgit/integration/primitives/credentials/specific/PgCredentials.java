package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.credentials.SimpleCredentials;

public class PgCredentials extends CredentialsEnvelope {
    public PgCredentials() {
        super(() -> {
            try {
                final Credentials creds = new PgPropsCredentials();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            } catch (Throwable e) {
                final Credentials creds = new PgFileCredentials();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            }
        });
    }
}
