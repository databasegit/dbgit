package ru.fusionsoft.dbgit.integration.primitives.connection;

import java.nio.file.Path;

public class ConnectionFromDbGitRepo extends ConnectionEnvelope {
    public ConnectionFromDbGitRepo(Path pathToDbGitRepo) {
        super(()->new ConnectionFromFileDbLink(pathToDbGitRepo.resolve(".dbgit/.dblink")));
    }
}
