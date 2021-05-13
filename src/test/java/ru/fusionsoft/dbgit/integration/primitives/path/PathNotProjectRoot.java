package ru.fusionsoft.dbgit.integration.primitives.path;


import java.nio.file.Path;

public class PathNotProjectRoot extends PathEnvelope{
    public PathNotProjectRoot(Path origin) {
        super(()-> {
            if(
                origin.resolve(".git").toFile().exists() &&
                origin.resolve("pom.xml").toFile().exists()
            ){
                throw new PathIsProjectRootException(origin);
            }
            return origin;
        });
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


