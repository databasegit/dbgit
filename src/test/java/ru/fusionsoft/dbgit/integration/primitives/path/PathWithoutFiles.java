package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFiles;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFilesWildcard;

public class PathWithoutFiles extends PathEnvelope {

    public PathWithoutFiles(String[] names, Path origin) {
        super(
            () -> {
                new PathPatchDeletingFiles(names).apply(origin);
                return origin;
            }
        );
    }
    
    public PathWithoutFiles(String filterMask, Path origin) {
        super(
            () -> {
                new PathPatchDeletingFilesWildcard(filterMask).apply(origin);
                return origin;
            }
        );
    }
}
