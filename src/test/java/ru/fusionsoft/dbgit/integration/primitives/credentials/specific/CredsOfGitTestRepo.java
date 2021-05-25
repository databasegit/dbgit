package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.credentials.SimpleCredentials;

public class CredsOfGitTestRepo extends CredentialsEnvelope {
    public CredsOfGitTestRepo() {
        super(() -> {
            try {
                final Credentials creds = new CredsFromGitMvnDProps();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            } catch (Throwable e) {
                final Credentials creds = new CredsFromGitSecretFile();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            }
        });
    }
}
