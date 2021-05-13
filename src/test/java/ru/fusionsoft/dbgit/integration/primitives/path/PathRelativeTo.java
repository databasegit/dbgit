package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathRelativeTo extends PathEnvelope {
    public PathRelativeTo(Path to, Path origin) {
        super(()-> {
            return Paths.get(
                to
                .toAbsolutePath()
                .toString()
            )
            .relativize(Paths.get(
                origin
                .toAbsolutePath()
                .toString()
            ));
        });
    }
}
