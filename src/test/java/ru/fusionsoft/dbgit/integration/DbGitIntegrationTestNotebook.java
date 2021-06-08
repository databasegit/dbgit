package ru.fusionsoft.dbgit.integration;

import java.nio.file.Path;
import java.text.MessageFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLinkPgAuto;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchRunningGit;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchGitCheckoutOrphan;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterGitRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios.PathAfterDbGitLinkAndAdd;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathWithDbGitRepoCloned;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.DescribedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTest;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAddRemoteTestRepo;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLinkPgLocal;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit.CharsDbIgnoreWithTableData;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.UrlOfGitTestRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitCheckout;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.ProjectTestResourcesCleanDirectoryPath;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios.PathAfterDbGitDumpsDbSchemaToGit;

@Tag("notebook")
@Disabled
public class DbGitIntegrationTestNotebook {
    @Test
    public final void appendsDbSchemaToBranchOfTestRepo() {
        final String description = "Appends db schema to existing git branch";
        final String nameOfBranchToAppendTo = "sakilla-data";
        final String nameOfDbToDump = "dvdrental";
        final String nameOfRemote = "origin-test";
        final CharSequence ignoreChars = new CharsDbIgnoreWithTableData();
        final DescribedTestResult<Path> result = new DescribedTestResult<>(
            description,
            new SimpleTestResult<>(
                new PathAfterDbGitDumpsDbSchemaToGit(
                    nameOfDbToDump,
                    nameOfRemote,
                    new UrlOfGitTestRepo(),
                    new ArgsDbGitLinkPgLocal(nameOfDbToDump),
                    new ArgsDbGitAddRemoteTestRepo(nameOfRemote),
                    ignoreChars,
                    new PathPatchDbGitCheckout(
                        nameOfBranchToAppendTo,
                        "-b",
                        "-nodb",
                        "-v"
                    ),
                    new DefaultPrintStream(),
                    new ProjectTestResourcesCleanDirectoryPath(description)
                ),
                new SimpleTest<>(
                    (path) -> {
                        return path.resolve(".dbgit/public/rental.csv").toFile().exists();
                    }
                )
            )
        );

        System.out.println("\n" + result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void dumpsDbSchemaToNewBranchOfTestRepo() {
        final String nameOfNewBranch = "sakilla-data";
        final String nameOfDbToDump = "pagilla";
        final String nameOfRemote = "origin-test";
        final String description = "Dumps db schema to new git branch";
        final CharSequence ignoreChars = new CharsDbIgnoreWithTableData();

        new PathAfterGitRun(
            new ArgsExplicit("push", nameOfRemote, nameOfNewBranch, "-v"),

            new PathAfterDbGitRun(
                new ArgsExplicit("commit", "-m", nameOfDbToDump),

                new PathAfterDbGitLinkAndAdd(
                    new ArgsDbGitLinkPgAuto(nameOfDbToDump),
                    ignoreChars,

                    new PathPatched(
                        new PathPatchGitCheckoutOrphan(nameOfNewBranch),

                        new PathAfterDbGitRun(
                            new ArgsDbGitAddRemoteTestRepo(nameOfRemote),

                            new PathWithDbGitRepoCloned(
                                new UrlOfGitTestRepo(),
                                
                                new ProjectTestResourcesCleanDirectoryPath(
                                    description
                                )
                            )
                        )
                    )
                )
            )
        ).toString();

    }

    @Test
    public final void cleansBranch() {
        final String branchToClean = "master";
        final String nameOfNewBranch = "orphan";
        final String nameOfRemote = "origin";
        final String description = "Replaces branch "
                                   + branchToClean
                                   + " with a new git branch";

        new PathPatched(
            new PatchSequential<>(
                new PathPatchGitCheckoutOrphan(nameOfNewBranch),
                new PathPatchCreatingFile("readme.md", "Just a clean branch"),
                new PathPatchRunningGit("add", "readme.md"),
                new PathPatchRunningGit("commit", "-m", "\"clean branch\""),
                new PathPatchRunningGit("push", nameOfRemote, 
                    MessageFormat.format(
                    "+{0}:{1}", nameOfNewBranch, branchToClean
                    )
                )
            ),
            new PathAfterDbGitRun(
                new ArgsDbGitAddRemoteTestRepo(nameOfRemote),

                new PathWithDbGitRepoCloned(
                    new UrlOfGitTestRepo(),
                    new ProjectTestResourcesCleanDirectoryPath(
                        description
                    )
                )
            )

        ).toString();

    }

}
