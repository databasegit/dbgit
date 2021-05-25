package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;

public class PathWithFiles extends PathPatched {
    public PathWithFiles(PathPatchCreatingFile[] filePatches, Path origin) {
        super(origin, new PatchSequential<Path>(filePatches));
    }
    public PathWithFiles(PathPatchCreatingFile filePatch, Path origin){
        super(filePatch, origin);
    }
}
