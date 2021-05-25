package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsWithPrepend;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCloningGitRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFiles;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFilesWildcard;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterProcessRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathOfBuiltDbGitExecutable;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;

public class PathWithBuildingDbGitExecutableFromGit extends PathEnvelope {

    public PathWithBuildingDbGitExecutableFromGit(String commitHash, PrintStream printStream, Path origin) {
        super(() -> {
            
            return new PathOfBuiltDbGitExecutable(
                new PathAfterProcessRun(
                    new ArgsWithPrepend(
                        new ArgsExplicit(
                            "mvn",
                            "package",
                            "appassembler:assemble",
                            "-D",
                            "skipTests"
                        ),
                        new ArgsExplicit(
                            System.getenv("ComSpec"),
                            "/C"
                        )
                    ),
                    printStream,
                    new PathPatched(
                        new PathNotProjectRoot(origin),
                        new PatchSequential<>(
                            new PathPatchDeletingFilesWildcard(
                                "*"
                            ),
                            new PathPatchCloningGitRepo(
                                "https://github.com/databasegit/dbgit.git",
                                commitHash,
                                printStream
                            ),
                            new PathPatchDeletingFiles(
                                "src/test/java/ru/fusionsoft/dbgit/mssql/DBAdapterMssqlTest.java"
                            )
                        )
                    )
                )
            );
            
        });
    }

    public PathWithBuildingDbGitExecutableFromGit(String commitHash, Path origin) {
        this(commitHash, new DefaultPrintStream(), origin);
    }
}
