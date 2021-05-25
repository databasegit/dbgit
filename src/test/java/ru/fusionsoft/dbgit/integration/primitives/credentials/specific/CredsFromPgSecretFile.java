package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsFromFile;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class CredsFromPgSecretFile extends CredentialsFromFile {
    public CredsFromPgSecretFile() {
        super(new CurrentWorkingDirectory().resolve("../pgSecret.txt"));
    }
}
