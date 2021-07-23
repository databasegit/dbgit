package ru.fusionsoft.dbgit.integration.primitives.files;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;

public class TempFilePath extends PathEnvelope {

    public TempFilePath(CharSequence fileName) {
        super(() -> Paths.get(String.valueOf(fileName)));
    }

    public TempFilePath(Path directory, CharSequence prefix) {
        this(
            new CharsOf<>(() -> {
                return directory.resolve(
                    prefix +
                    String.format("#%06x", new Random().nextInt(256 * 256 * 256))
                )
                    .toAbsolutePath()
                    .toString();
            })
        );
    }
}
