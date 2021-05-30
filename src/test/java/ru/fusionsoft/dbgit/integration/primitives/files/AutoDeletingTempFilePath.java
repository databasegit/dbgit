package ru.fusionsoft.dbgit.integration.primitives.files;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class AutoDeletingTempFilePath extends PathEnvelope implements AutoCloseable {
    
    public AutoDeletingTempFilePath(CharSequence fileName) {
        super(()-> Paths.get(String.valueOf(fileName)));
    }
    public AutoDeletingTempFilePath(Path directory, CharSequence prefix) {
        this(
            new CharsOf<>( ()-> {
                return directory.resolve(
                    prefix +
                    String.format("#%06x", new Random().nextInt(256 * 256 * 256))
                )
                .toAbsolutePath()
                .toString();
            })
        );
    }
    
    @Override
    public final void close() throws IOException {
        Files.deleteIfExists(this.toFile().toPath());
    }
}
