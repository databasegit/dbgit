package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.credentials.SimpleCredentials;

public class GitTestRepoCredentials extends CredentialsEnvelope {
    public GitTestRepoCredentials() {
        super(() -> {
            try {
                final Credentials creds = new GitTestRepoPropsCredentials();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            } catch (Throwable e) {
                final Credentials creds = new GitTestRepoFileCredentials();
                return new SimpleCredentials(
                    creds.username(),
                    creds.password()
                );
            }
        });
    }
}
