package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.PatchedScalar;

public class PathPatched extends PathEnvelope {

    public PathPatched(final Path origin, final Patch<Path> patch) {
        super(
            new PatchedScalar<>(origin, patch)
        );
    }
}
