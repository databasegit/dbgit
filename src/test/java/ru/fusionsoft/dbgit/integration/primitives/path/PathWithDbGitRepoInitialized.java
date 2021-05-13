package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;

public class PathWithDbGitRepoInitialized extends PathAfterDbGitRun {
    public PathWithDbGitRepoInitialized(CharSequence repoUrl, Path origin) {
        super(
            new ArgsExplicit("init"),

            new PathAfterDbGitRun(
                new ArgsExplicit(
                    "clone",
                    repoUrl,
                    "--directory",
                    "\"" + origin.toAbsolutePath().toString() + "\""
                ),

                new PathWithoutFiles(
                    "*",
                    new PathNotProjectRoot(
                        origin
                    )
                )
                
            )
        );
    }
}
