package ru.fusionsoft.dbgit.integration;

import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.TestResult;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCloningGitRepo;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchDeletingFiles;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPrintsToConsole;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.ProjectTestResourcesCleanDirectoryPath;
import ru.fusionsoft.dbgit.integration.primitives.DescribedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.GrouppedTR;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequental;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTest;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;

public class ClassCompositionTest {

    @Test
    public final void subjectSideEffectAppearsOnceWhenAccessingTwice() throws Exception {
        Assertions.assertEquals(
            2,
            new DescribedTestResult(
                "Test result subject side effect appears once",
                new SimpleTestResult<Path>(
                    new PathPrintsToConsole(
                        "Line printed as side effect...",
                        new ProjectTestResourcesCleanDirectoryPath(
                            "Test result subject side effect appears once"
                        )
                    ),
                    subj -> {
                        subj.isAbsolute();
                        subj.toString();
                        subj.toFile();
                        return true;
                    }
                )
            )
            .text()
            .split("\n")
            .length
        );
    }

    @Test
    public final void failsToFalseOnException() {
        final ProjectTestResourcesCleanDirectoryPath workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath(
                "Fails to false on exception");

        Assertions.assertDoesNotThrow(
            () -> {
                final TestResult testResult =
                new DescribedTestResult(
                    "Fails to false on exception",
                    new SimpleTestResult<Path>(
                        new PathPatched(
                            new PathNotProjectRoot(workingDirectory),
                            new Patch<Path>() {
                                @Override
                                public void apply(Path root) throws Exception {
                                    System.out.println("access " + root.toString());
                                    throw new Error(
                                        "dummy error"
                                    );
                                }
                            }
                        ),
                        path -> {
                            path.toString();
                            path.toString();
                            path.toString();
                            path.toString();
                            return ! path
                                .resolve("pom.xml")
                                .toFile()
                                .exists();
                        }
                    )
                );

                System.out.println(testResult.text());
                Assertions.assertFalse(testResult.value());
            }
        );
    }

    @Test
    public final void groupedTestResultWorks() {
        final DescribedTestResult result = new DescribedTestResult(
            "Grouped test result works",
            new GrouppedTR<Path>(
                new ProjectTestResourcesCleanDirectoryPath(
                    "Grouped test result works"
                ),
                new SimpleTest<>(
                    "Retruns true",
                    (path) -> {
                        path.toString();
                        return true;
                    }
                ),
                new SimpleTest<>(
                    "Throws exception",
                    (path) -> {
                        path.toString();
                        throw new Exception("dummy expection");
                    }
                ),
                new SimpleTest<>(
                    "Throws error",
                    (path) -> {
                        path.toString();
                        throw new Error("dummy error the second");
                    }
                )
            )
        );
        System.out.println(result.text());
        Assertions.assertFalse(result.value());
    }

    @Test
    public final void patchedWorks() {
        final ProjectTestResourcesCleanDirectoryPath workingDirectory =
            new ProjectTestResourcesCleanDirectoryPath(
                "Patched works"
            );

        final String fileName = "testFile.txt";
        final String content = "content";

        final DescribedTestResult result = new DescribedTestResult(
            "Patched works",
            new GrouppedTR<>(
                new PathPatched(
                    new PathNotProjectRoot(workingDirectory),
                    new PathPatchCreatingFile(fileName, content)
                ),

                new SimpleTest<>(
                    "Файл существует",
                    path -> {
                        return path.resolve(fileName).toFile().exists();
                    }
                ),
                new SimpleTest<>(
                    "Содержимое файла как ожидалось",
                    path -> {
                        return FileUtils.readFileToString(
                            path.resolve(fileName).toFile()
                        )
                            .contains(content);
                    }
                )
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void patchSequentalWorks() {
        final DescribedTestResult result = new DescribedTestResult(
            "Sequental patch works",
            new SimpleTestResult<>(
                new PathPatched(
                    new PathNotProjectRoot(
                        new ProjectTestResourcesCleanDirectoryPath(
                            "Sequental patch works"
                        )
                    ),
                    new PatchSequental<>(
                        new PathPatchDeletingFiles(".git", ".dbgit"),
                        new PathPatchCloningGitRepo(
                            "https://github.com/rocket-3/dbgit-test.git",
                            "master"
                        )
                    )
                ),
                new SimpleTest<>(
                    "Git folder exists",
                    x -> x.resolve(".git").toFile().exists()
                )
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());

    }

}
