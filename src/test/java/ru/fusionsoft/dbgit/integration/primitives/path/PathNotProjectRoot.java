package ru.fusionsoft.dbgit.integration.primitives.path;


import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchAssertsNotMvnProjectRoot;

public class PathNotProjectRoot extends PathPatched{
    public PathNotProjectRoot(Path origin) {
        super(new PathPatchAssertsNotMvnProjectRoot(), origin);
    }


}


