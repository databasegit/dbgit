package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public class PathPatchAssertsNotMvnProjectRoot implements Patch<Path> {
    @Override
    public final void apply(Path root) throws Exception {
        if (
            root.resolve(".git").toFile().exists() &&
            root.resolve("pom.xml").toFile().exists()
        ) {
            throw new PathIsProjectRootException(root);
        }
    }

    private static class PathIsProjectRootException extends RuntimeException {
        PathIsProjectRootException(Path path) {
            super(
                "\nGiven path " + path.toString() + " " +
                "points to a project root directory.\n" +
                "I'm here not to allow that."
            );
        }
    }
}
