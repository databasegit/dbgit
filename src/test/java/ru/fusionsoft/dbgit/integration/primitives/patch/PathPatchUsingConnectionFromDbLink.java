package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.nio.file.Path;
import java.sql.Connection;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.connection.ConnectionFromDbGitRepo;

public class PathPatchUsingConnectionFromDbLink implements Patch<Path> {
    private final Patch<Connection> connectionPatch;

    public PathPatchUsingConnectionFromDbLink(Patch<Connection> connectionPatch) {
        this.connectionPatch = connectionPatch;
    }

    @Override
    public final void apply(Path root) throws Exception {
        connectionPatch.apply(new ConnectionFromDbGitRepo(root));
    }
}
