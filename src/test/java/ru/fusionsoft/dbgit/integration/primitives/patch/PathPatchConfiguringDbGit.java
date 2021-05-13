package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequental;

public class PathPatchConfiguringDbGit extends PatchSequental<Path> {

    public PathPatchConfiguringDbGit(final String linkContent, final String ignoreContent, final String configContent, final String indexContent) {
        super(
            new PathPatchCreatingFile(".dbgit/.dblink", linkContent),
            new PathPatchCreatingFile(".dbgit/.dbignore", ignoreContent),
            new PathPatchCreatingFile(".dbgit/.dbindex", indexContent),
            new PathPatchCreatingFile(".dbgit/dbgitconfig", configContent)
        );

    }

}
