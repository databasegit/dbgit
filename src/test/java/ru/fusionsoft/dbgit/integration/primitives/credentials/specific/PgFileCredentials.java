package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.FromFileCredentials;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class PgFileCredentials extends FromFileCredentials {
    public PgFileCredentials() {
        super(new CurrentWorkingDirectory().resolve("../pgSecret.txt"));
    }
}
