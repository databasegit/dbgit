package ru.fusionsoft.dbgit.integration.primitives.files;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class AutoDeletingPath extends PathEnvelope implements AutoCloseable {
    
    public AutoDeletingPath(Path origin) {
        super( () -> origin );
    }
    
    @Override
    public final void close() throws IOException {
        Files.deleteIfExists(this.toFile().toPath());
    }
}
