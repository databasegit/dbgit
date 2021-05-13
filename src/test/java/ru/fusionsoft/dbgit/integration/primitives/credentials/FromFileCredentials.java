package ru.fusionsoft.dbgit.integration.primitives.credentials;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.files.FileContent;

public class FromFileCredentials extends CredentialsEnvelope {

    public FromFileCredentials(Path secretFilePath) {
        super(()-> {
            final String[] lines = new FileContent(secretFilePath)
            .text()
            .split("\n");
            return new SimpleCredentials(lines[0],lines[1]);
        });
    }
}
