package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.File;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public class PathPatchCreatingFile implements Patch<Path> {
    private final String name;
    private final String content;

    public PathPatchCreatingFile(final String name, final CharSequence content) {
        this.name = name;
        this.content = String.valueOf(content);
    }

    @Override
    public final void apply(final Path root) throws Exception {
        final File file = root.resolve(this.name).toFile();
        file.getParentFile().mkdirs();
        FileUtils.writeStringToFile(file, this.content);
    }
}
