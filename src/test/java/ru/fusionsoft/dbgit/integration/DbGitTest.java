package ru.fusionsoft.dbgit.integration;

import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.DescribedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTest;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;
import ru.fusionsoft.dbgit.integration.primitives.TestResult;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsCheckoutNodb;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsLink;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.AutoPgLinkArgs;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.GitTestRepoAddRemoteArgs;
import ru.fusionsoft.dbgit.integration.primitives.chars.SavedConsoleText;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRestore;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithBuildingDbGitExecutableFromGit;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithDbGitRepoInitialized;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithFiles;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithoutFiles;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.ProjectTestResourcesCleanDirectoryPath;

@Tag("integration")
public class DbGitTest {

    @Test
    public final void clonesRepoAndReturnsCurrentCommitNumber() throws Exception {
        final Path workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath(
                "Clones repo and prints expected commit hash")
                    .toAbsolutePath();

        final String commitHash = "b1fecd7";
        
        final TestResult testResult = new DescribedTestResult<>(
            "Dbgit clone works as expected",
            new SavedConsoleText(
                () -> {
                    new PathAfterDbGitRun(
                        new ArgsExplicit(
                            "checkout",
                            "-ls", "-v"
                        ),
                        System.out,
                        new PathAfterDbGitRun(
                            new ArgsExplicit(
                                "checkout",
                                "master",
                                commitHash,
                                "-nodb"
                            ),
                            new PathAfterDbGitRun(
                                new GitTestRepoAddRemoteArgs("origin"),

                                new PathAfterDbGitRun(
                                    new ArgsExplicit("init"),

                                    new PathAfterDbGitRun(
                                        new ArgsExplicit(
                                            "clone",
                                            "https://github.com/rocket-3/dbgit-test.git",
                                            "--directory",
                                            "\"" + workingDirectory
                                                .toString() + "\""
                                        ),

                                        new PathWithoutFiles(
                                            "*",
                                            new PathNotProjectRoot(
                                                workingDirectory
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    ).toString();
                }
            ),
            new SimpleTest<SavedConsoleText>(
                "Printed expected commit hash",
                (text) -> {
                    return text
                        .lines()
                        .stream()
                        .anyMatch(
                            (line) -> line.contains(commitHash)
                        );
                }
            )
        );

        System.out.println(testResult.text());
        Assertions.assertTrue(testResult.value());
    }

    @Test
    public final void fetchesDatabaseObjects() {
        final Path workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath("fetchesDatabaseObjects")
                .toAbsolutePath();

        final DescribedTestResult result = new DescribedTestResult<>(
            "Dbgit add command test",
            new SimpleTestResult<>(
                new PathAfterDbGitRun(
                    new ArgsExplicit("add", "\"*\""),
                    new PathAfterDbGitRun(
                        new AutoPgLinkArgs("pagilla"),
                        new PathAfterDbGitRun(
                            new ArgsExplicit("init"),
                            new PathWithoutFiles(
                                "*",
                                new PathNotProjectRoot(
                                    workingDirectory
                                )
                            )
                        )
                    )
                ),
                new SimpleTest<>(
                    (path) -> {
                        return path.resolve(".git").toFile().exists();
                    }
                )
            )
        );

        System.out.println("\n" + result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void fetchesAndCommitsWholeNewStructure() {
        final Path workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath(
                "fetchesAndCommitsWholeNewStructure")
                .toAbsolutePath();

        final DescribedTestResult result = new DescribedTestResult(
            "Dbgit add command test",
            new SimpleTestResult<>(
                new PathAfterDbGitRun(
                    new ArgsExplicit("push"),
                    new PathAfterDbGitRun(
                        new ArgsExplicit(
                            "commit",
                            "-m",
                            "Pagilla database"
                        ),
                        new PathAfterDbGitRun(
                            new ArgsExplicit("add", "\"*\""),
                            // dbgit rm -idx 
                            // can't work without MetaFile parsing, which
                            // is unstable during current development 
                            //new PathAfterAppRunInProcess(new ExplicitArgs("rm","\"*\"", "-idx"),
                            new PathWithFiles(
                                new PathPatchCreatingFile(
                                    ".dbgit/.dbindex",
                                    "version=0.3.1"
                                ),
                                new PathWithoutFiles(
                                    new String[]{".dbgit/public"},
                                    new PathAfterDbGitRun(
                                        new ArgsExplicit(
                                            "checkout",
                                            "ng",
                                            "-nodb"
                                        ),
                                        new PathAfterDbGitRun(
                                            new AutoPgLinkArgs("dvdrental"),
                                            new PathAfterDbGitRun(
                                                new ArgsExplicit(
                                                    "checkout",
                                                    "master",
                                                    "-nodb"
                                                ),
                                                new PathAfterDbGitRun(
                                                    new GitTestRepoAddRemoteArgs("origin"),
                                                    new PathAfterDbGitRun(
                                                        new ArgsExplicit("init"),
                                                        new PathAfterDbGitRun(
                                                            new ArgsExplicit(
                                                                "clone",
                                                                "https://github.com/rocket-3/dbgit-test.git",
                                                                "--directory",
                                                                "\""
                                                                + workingDirectory
                                                                    .toString()
                                                                + "\""
                                                            ),
                                                            new PathWithoutFiles(
                                                                "*",
                                                                new PathNotProjectRoot(
                                                                    workingDirectory
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )

                ),
                new SimpleTest<>(
                    (path) -> {
                        return path.resolve(".git").toFile().exists();
                    }
                )
            )
        );

        System.out.println("\n" + result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void commitsLilChangedDbSchema() {
        final Path workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath(
                "commitsLilChangedDbSchema")
                    .toAbsolutePath();

        final DescribedTestResult<Path> result = new DescribedTestResult<>(
            "Dbgit add command test",
            new SimpleTestResult<>(
                new PathAfterDbGitRun(
                    new ArgsExplicit(
                        "push"
                    ),

                    new PathAfterDbGitRun(
                        new ArgsExplicit(
                            "commit",
                            "-m",
                            "Sakilla database"
                        ),

                        new PathAfterDbGitRun(
                            new ArgsExplicit(
                                "add",
                                "\"*\""
                            ),

                            new PathAfterDbGitRun(
                                new AutoPgLinkArgs("dvdrental"),

                                new PathAfterDbGitRun(
                                    new ArgsExplicit(
                                        "checkout",
                                        "ng",
                                        "-nodb"
                                    ),

                                    new PathAfterDbGitRun(
                                        new GitTestRepoAddRemoteArgs("origin"),

                                        new PathAfterDbGitRun(
                                            new ArgsExplicit("init"),

                                            new PathAfterDbGitRun(
                                                new ArgsExplicit(
                                                    "clone",
                                                    "https://github.com/rocket-3/dbgit-test.git",
                                                    "--directory",
                                                    "\"" + workingDirectory
                                                        .toString() + "\""
                                                ),

                                                new PathWithoutFiles(
                                                    "*",
                                                    new PathNotProjectRoot(
                                                        workingDirectory
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )

                            )
                        )
                    )

                ),
                new SimpleTest<>(
                    (path) -> {
                        return path.resolve(".git").toFile().exists();
                    }
                )
            )
        );

        System.out.println("\n" + result.text());
        Assertions.assertTrue(result.value());

    }

    @Test
    public final void usesAnotherDbGitVersionToRestore() {

        final Path olderDbGitExecutablePath = new PathWithBuildingDbGitExecutableFromGit(
            "51e8fa0",
            new ProjectTestResourcesCleanDirectoryPath("dbgit version 51e8fa0")
        );
        final ArgsLink testDbLinkArgsMoniker = new AutoPgLinkArgs("test#databasegit");
        final TestResult result = new DescribedTestResult<>(
            "Uses older dbgit version to prepare database",
            new SimpleTestResult<>(
                new PathAfterDbGitRestore(
                    new ArgsCheckoutNodb("master", "8867384"),
                    testDbLinkArgsMoniker,
                    olderDbGitExecutablePath,
                    new PathAfterDbGitRestore(
                        new ArgsCheckoutNodb("master", "b1fecd7"),
                        testDbLinkArgsMoniker,
                        olderDbGitExecutablePath,
                        new PathAfterDbGitRestore(
                            new ArgsCheckoutNodb("master", "831054e"),
                            testDbLinkArgsMoniker,
                            olderDbGitExecutablePath,
                            new PathAfterDbGitRestore(
                                new ArgsCheckoutNodb("master", "d2b4080"),
                                testDbLinkArgsMoniker,
                                olderDbGitExecutablePath,

                                new PathAfterDbGitRestore(
                                    new ArgsCheckoutNodb("master", "4f3953d"),
                                    testDbLinkArgsMoniker,
                                    olderDbGitExecutablePath,

                                    new PathWithDbGitRepoInitialized(
                                        "https://github.com/rocket-3/dbgit-test.git",

                                        new ProjectTestResourcesCleanDirectoryPath(
                                            "usesAnotherDbGitVersionToRestore"
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
                new SimpleTest<>(
                    "Exception not thrown",
                    (path) -> {
                        path.toString();
                        return true;
                    }
                )
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void createsAndUpdatesDatabase() {
        final String description = "Fills and updates database from different repos";
        final Path workingDirectory = new ProjectTestResourcesCleanDirectoryPath(
            description
        )
        .toAbsolutePath();


        final TestResult result = new DescribedTestResult(
            description,
            new SimpleTestResult<>(
                new PathAfterDbGitRun(
                    new ArgsExplicit("restore", "-r", "-v"),

                    new PathAfterDbGitRun(
                        new AutoPgLinkArgs("test#databasegit"),

                        new PathAfterDbGitRun(
                            new ArgsExplicit("add", "\"*\""),

                            new PathAfterDbGitRun(
                                new AutoPgLinkArgs("pagilla"),
                                new PathAfterDbGitRun(
                                    new ArgsExplicit("restore", "-r", "-v"),
                                    System.out,
                                    new PathAfterDbGitRun(
                                        new AutoPgLinkArgs("test#databasegit"),

                                        new PathAfterDbGitRun(
                                            new ArgsExplicit("add", "\"*\""),

                                            new PathAfterDbGitRun(
                                                new AutoPgLinkArgs("dvdrental"),

                                                new PathAfterDbGitRun(
                                                    new ArgsExplicit("init"),

                                                    new PathWithoutFiles(
                                                        "*",
                                                        new PathNotProjectRoot(
                                                            workingDirectory
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
                new SimpleTest<>(
                    "Exception not thrown",
                    (path) -> {
                        path.toString();
                        return true;
                    }
                )
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

}
