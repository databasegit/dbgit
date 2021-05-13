package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;

public class PathWithFiles extends PathEnvelope {
    public PathWithFiles(PathPatchCreatingFile[] filePatches, Path origin) {
        super(new Scalar<Path>() {
            @Override
            public Path value() throws Exception {
                for (final PathPatchCreatingFile filePatch : filePatches) {
                    filePatch.apply(origin);
                }
                return origin;
            }
        });
    }
    public PathWithFiles(PathPatchCreatingFile file, Path origin){
        this(new PathPatchCreatingFile[]{file}, origin);
    }
}
