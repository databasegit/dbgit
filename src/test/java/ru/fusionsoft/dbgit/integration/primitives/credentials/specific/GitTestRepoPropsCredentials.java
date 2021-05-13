package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.FromPropertiesCredentials;

public class GitTestRepoPropsCredentials extends FromPropertiesCredentials {
    public GitTestRepoPropsCredentials() {
        super("gitUser", "gitPass");
    }
}
