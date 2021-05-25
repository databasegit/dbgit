package ru.fusionsoft.dbgit.integration.primitives.credentials.specific;

import ru.fusionsoft.dbgit.integration.primitives.credentials.CredentialsFromFile;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class CredsFromGitSecretFile extends CredentialsFromFile {
    public CredsFromGitSecretFile() {
        super(new CurrentWorkingDirectory().resolve("../gitSecret.txt"));
    }
}
