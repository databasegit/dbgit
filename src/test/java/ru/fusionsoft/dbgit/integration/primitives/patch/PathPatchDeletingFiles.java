package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.FileUtils;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public class PathPatchDeletingFiles implements Patch<Path> {
    private final Collection<String> names;

    private PathPatchDeletingFiles(Collection<String> names) {
        this.names = names;
    }

    public PathPatchDeletingFiles(String... names) {
        this(Arrays.asList(names));
    }

    @Override
    public final void apply(Path root) throws Exception {
        for (String name : this.names) {
            final File file = root.resolve(name).toFile();
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
        }
    }
}
