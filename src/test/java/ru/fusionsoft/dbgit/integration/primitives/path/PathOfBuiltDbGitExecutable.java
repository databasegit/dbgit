package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;

public class PathOfBuiltDbGitExecutable extends PathOf {
    public PathOfBuiltDbGitExecutable(Path dbGitProjectPath) {
        super(
            "target/dbgit/bin/dbgit", 
            dbGitProjectPath
        );
    }
}
