package ru.fusionsoft.dbgit.integration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitCheckout;
import ru.fusionsoft.dbgit.integration.primitives.DescribedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.GroupedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTest;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;
import ru.fusionsoft.dbgit.integration.primitives.TestResult;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLinkPgAuto;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitAddRemoteTestRepo;
import ru.fusionsoft.dbgit.integration.primitives.chars.CommitsFromRepo;
import ru.fusionsoft.dbgit.integration.primitives.chars.LinesOfUnsafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit.CharsDbGitConfigBackupEnabled;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitCheckout;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitCheckoutHard;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitClonesRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitRestore;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfConsoleWhenRunning;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit.CharsDbIgnoreWithTableData;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.NameOfDefaultTargetTestDatabase;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.UrlOfGitTestRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitAdd;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitInitializesRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithFiles;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios.PathAfterDbGitLinkAndAdd;
import ru.fusionsoft.dbgit.integration.primitives.path.PathAfterDbGitRun;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.PathWithDbGitRepoCloned;
import ru.fusionsoft.dbgit.integration.primitives.path.PathWithoutFiles;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.ProjectTestResourcesCleanDirectoryPath;

@Tag("integration")
public class DbGitIntegrationTestBasic {

    @Test
    public final void doesNothingIntegrationTestTemplate() {
        final String description = "Does nothing test works";
        final TestResult result = 
        new GroupedTestResult<>(
            description,
            new PathPatched(
                new ProjectTestResourcesCleanDirectoryPath("00"),
                new Patch<Path>() {
                    @Override
                    public void apply(Path root) throws Exception {

                    }
                }
            ),
            new SimpleTest<>(
                "Returns true",
                (path) -> {
                    path.toString();
                    return true;
                }
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void gitToFilesCheckoutWorks() {
        final String description = "Dbgit checkout and reset works";
        final String branchName = "dsdata";
        final String commitNameOfHead = "9e91d86";
        final String commitNameOfDs2 = "83baf51";
        final String fileName = ".dbgit/.dblink";
        final String reamdeFileName = "README.md";
        final String expectedFileContent = "ds2";
        final TestResult testResult = new GroupedTestResult<>(
            description,

            new PathAfterDbGitRun(
                new ArgsDbGitCheckout(commitNameOfDs2, "-nodb", "-v"),

                new PathAfterDbGitRun(
                    new ArgsDbGitCheckout(branchName, "-b", "-nodb", "-v"),

                    new PathAfterDbGitRun(
                        new ArgsDbGitAddRemoteTestRepo("origin"),

                        new PathWithDbGitRepoCloned(
                            new UrlOfGitTestRepo(),
                            new ProjectTestResourcesCleanDirectoryPath("01")
                        )
                    )
                )
            ),
            new SimpleTest<>(
                "Printed expected commit hash",
                (path) -> {
                    final CharSequence consoleOutput = new CharsOfConsoleWhenRunning(() -> {
                        new PathAfterDbGitRun(new ArgsExplicit("checkout", "-ls", "-v"), path).toString();
                    });
                    System.out.println(consoleOutput);
                    return new LinesOfUnsafeScalar(consoleOutput)
                    .list()
                    .stream()
                    .anyMatch(x -> x.contains(commitNameOfDs2));
                }
            ),
            new SimpleTest<>(
                "File content of "+fileName+" is as expected",
                (path) -> {
                    return Files.readAllLines(path.resolve(fileName))
                    .stream()
                    .anyMatch(x->x.contains(expectedFileContent));
                }
            ),
            new SimpleTest<>(
                "File "+reamdeFileName+" does not exists",
                (path) -> {
                    return ! path.resolve("README.md").toFile().exists();
                }
            ),
            new SimpleTest<>(
                "File "+reamdeFileName+" exists after checkout back to ",
                (path) -> {
                    return new PathPatched(
                        new PatchSequential<Path>(
                            new PathPatchDbGitCheckout(branchName, "-nodb", "-v"),
                            new PathPatchDbGitCheckout("-ls", "-v")
                        ), 
                        path
                    ).resolve(reamdeFileName).toFile().exists();
                }
            )
        );

        System.out.println(testResult.text());
        Assertions.assertTrue(testResult.value());
    }

    @Test
    public final void dbToFilesDumpWorks() {
        final String description = "Dbgit add with table data works";
        final TestResult result = new GroupedTestResult<Path>(
            description,
            
            new PathAfterDbGitRun(
                new ArgsExplicit("add", "\"*\""),

                new PathWithFiles(
                    new PathPatchCreatingFile(".dbgit/.dbignore", new CharsDbIgnoreWithTableData()),

                    new PathAfterDbGitRun(
                        new ArgsDbGitLinkPgAuto("pagilla"),

                        new PathAfterDbGitRun(
                            new ArgsExplicit("init"),

                            new ProjectTestResourcesCleanDirectoryPath("02")
                        )
                    )
                )
            ),

            new SimpleTest<>(
                "git folder exists",
                (path) -> {
                    return path.resolve(".git").toFile().exists();
                }
            ),
            new SimpleTest<>(
                "rental table data exists",
                (path) -> {
                    return path.resolve(".dbgit/public/rental.csv").toFile().exists();
                }
            ),
            new SimpleTest<>(
                "city table data (small) is not empty",
                (path) -> {
                    return ! Files.readAllLines(path.resolve(".dbgit/public/city.csv")).isEmpty();
                }
            ),
            new SimpleTest<>(
                "rental table data (10K+ rows) exists and is empty",
                (path) -> {
                    return Files.readAllLines(path.resolve(".dbgit/public/rental.csv")).isEmpty();
                }
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void dbToDbRestoreWorks() {
        final String nameOfSourceDb = "dsd3";
        final String nameOfTargetDb = new NameOfDefaultTargetTestDatabase().toString();
        final String description =
            "Simple add from '" + nameOfSourceDb + "' db with data " +
            "and restore to '" + nameOfTargetDb + "' db works";

        final TestResult result = new GroupedTestResult<>(
                description,
            new PathPatched(
                new PatchSequential<Path>(
                    new PathPatchDbGitInitializesRepo(new ArgsDbGitLinkPgAuto(nameOfSourceDb)),
                    new PathPatchCreatingFile(".dbgit/.dbignore", new CharsDbIgnoreWithTableData()),
                    new PathPatchDbGitAdd(),
                    new PathPatchDbGitLink(new ArgsDbGitLinkPgAuto(nameOfTargetDb)),
                    new PathPatchDbGitRestore("-r", "-v")
                ),
                new ProjectTestResourcesCleanDirectoryPath("03")
            ),
            new SimpleTest<>(
                "Should not throw any exceptions",
                (path) -> {
                    path.toString();
                    return true;
                }
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void gitToDbRestoreWorks() throws Exception {

        final String nameOfSourceBranch = "dsdata";
        final String description =
            "Simple sequential checkout from '" + nameOfSourceBranch + "' branch " +
            "and restore to '" + new NameOfDefaultTargetTestDatabase() + "' db works";

        final List<String> commitNames = new CommitsFromRepo(
            new UrlOfGitTestRepo().toString(), nameOfSourceBranch
        ).names();

        final ArgsDbGitLinkPgAuto linkArgs = new ArgsDbGitLinkPgAuto(new NameOfDefaultTargetTestDatabase());
        final TestResult result = new GroupedTestResult<>(
            description,
            new PathPatched(
                new PatchSequential<Path>(
                    new PathPatchDbGitClonesRepo(new UrlOfGitTestRepo(), new ArgsDbGitAddRemoteTestRepo()),
                    new PathPatchDbGitCheckout(nameOfSourceBranch, "-b", "-nodb"),
                    new PathPatchDbGitCheckout(commitNames.get(0), "-nodb", "-v"),
                    new PathPatchDbGitLink(linkArgs),
                    new PathPatchCreatingFile(".dbgit/dbgitconfig", new CharsDbGitConfigBackupEnabled()),
                    new PathPatchDbGitRestore("-r", "-v"),
                    new PathPatchDbGitCheckoutHard(commitNames.get(1), "-nodb", "-v"),
                    new PathPatchDbGitLink(linkArgs),
                    new PathPatchCreatingFile(".dbgit/dbgitconfig", new CharsDbGitConfigBackupEnabled()),
                    new PathPatchDbGitRestore("-r", "-v"),
                    new PathPatchDbGitCheckoutHard(commitNames.get(2), "-nodb", "-v"),
                    new PathPatchDbGitLink(linkArgs),
                    new PathPatchCreatingFile(".dbgit/dbgitconfig", new CharsDbGitConfigBackupEnabled()),
                    new PathPatchDbGitRestore("-r", "-v")
                ),
                new ProjectTestResourcesCleanDirectoryPath("04")
            ),
            new SimpleTest<>(
                "Should not throw any exceptions",
                (path) -> {
                    path.toString();
                    return true;
                }
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void dbToDbRestoreWorksWithCustomTypes() {
        final String description = "Hardest sakilla_database sequential add and restore with table data and custom types (mpaa_rating) works";
        final TestResult result = new DescribedTestResult<Path>(
            description,
            new SimpleTestResult<>(
                new PathAfterDbGitRun(
                    //pagilla to test#databasegit over dvdrental
                    new ArgsExplicit("restore", "-r", "-v"),

                    new PathAfterDbGitRun(
                        new ArgsDbGitLinkPgAuto(new NameOfDefaultTargetTestDatabase()),

                        //pagilla to local repo
                        new PathAfterDbGitLinkAndAdd(
                            new ArgsDbGitLinkPgAuto("pagilla"),
                            new CharsDbIgnoreWithTableData(),

                            //dvdrental to test#databasegit
                            new PathAfterDbGitRun(
                                new ArgsExplicit("restore", "-r", "-v"),

                                new PathAfterDbGitRun(
                                    new ArgsDbGitLinkPgAuto(new NameOfDefaultTargetTestDatabase()),

                                    //dvdrental to local repo
                                    new PathAfterDbGitLinkAndAdd(
                                        new ArgsDbGitLinkPgAuto("dvdrental"),
                                        new CharsDbIgnoreWithTableData(),

                                        new PathAfterDbGitRun(
                                            new ArgsExplicit("init"),

                                            new PathWithoutFiles(
                                                "*",
                                                new PathNotProjectRoot(
                                                    new ProjectTestResourcesCleanDirectoryPath("05")
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
                    "Should not throw any exceptions",
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
