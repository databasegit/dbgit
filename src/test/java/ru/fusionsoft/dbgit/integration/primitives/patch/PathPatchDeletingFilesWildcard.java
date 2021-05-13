package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public class PathPatchDeletingFilesWildcard implements Patch<Path> {
    private final FileFilter wildcardFileFilter;

    public PathPatchDeletingFilesWildcard(String mask) {
        this.wildcardFileFilter = new WildcardFileFilter(mask);
    }

    @Override
    public final void apply(Path root) throws Exception {
        final File[] files = root.toFile().listFiles((FilenameFilter) wildcardFileFilter);
        if(files != null)
            for (final File file : files) {
                FileUtils.forceDelete(file);
            }
    }
}
