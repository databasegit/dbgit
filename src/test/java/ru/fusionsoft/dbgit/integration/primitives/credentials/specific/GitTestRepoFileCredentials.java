package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.FromFileCredentials;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class GitTestRepoFileCredentials extends FromFileCredentials {
    public GitTestRepoFileCredentials() {
        super(new CurrentWorkingDirectory().resolve("../gitSecret.txt"));
    }
}
