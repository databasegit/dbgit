package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;

public class PathOf extends PathEnvelope {

    public PathOf(Scalar<Path> origin) {
        super(origin);
    }
    
    public PathOf(String path, Path origin) {
        super(()->origin.resolve(path));
    }
}
