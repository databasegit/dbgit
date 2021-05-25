package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.path.PathOf;

public class PathOfBuiltDbGitExecutable extends PathOf {
    public PathOfBuiltDbGitExecutable(Path dbGitProjectPath) {
        super(
            "target/dbgit/bin/dbgit", 
            dbGitProjectPath
        );
    }
}
