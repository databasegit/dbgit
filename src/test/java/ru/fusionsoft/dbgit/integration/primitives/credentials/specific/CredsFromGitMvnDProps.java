package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsFromProperties;

public class CredsFromGitMvnDProps extends CredentialsFromProperties {
    public CredsFromGitMvnDProps() {
        super("gitUser", "gitPass");
    }
}
