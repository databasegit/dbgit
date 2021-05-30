package ru.fusionsoft.dbgit.integration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.TestResult;
import ru.fusionsoft.dbgit.integration.primitives.files.AutoDeletingTempFilePath;
import ru.fusionsoft.dbgit.integration.primitives.patch.PathPatchCreatingFile;
import ru.fusionsoft.dbgit.integration.primitives.path.PathNotProjectRoot;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPrintsToConsole;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.ProjectTestResourcesCleanDirectoryPath;
import ru.fusionsoft.dbgit.integration.primitives.DescribedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.GroupedTestResult;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.PatchSequential;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTest;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;

public class SelfTest {

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
                    new SimpleTest<>(subj -> {
                        subj.isAbsolute();
                        subj.toString();
                        subj.toFile();
                        return true;
                    })
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
                                        System.out.println("access "
                                                           + root.toString());
                                        throw new Error(
                                            "dummy error"
                                        );
                                    }
                                }
                            ),
                            new SimpleTest<>(path -> {
                                path.toString();
                                path.toString();
                                path.toString();
                                path.toString();
                                return ! path
                                    .resolve("pom.xml")
                                    .toFile()
                                    .exists();
                            })
                        )
                    );

                Assertions.assertFalse(testResult.value());
            }
        );
    }

    @Test
    public final void groupedTestResultWorks() {
        final TestResult result = new GroupedTestResult<Path>(
            "Grouped test result works",
            new ProjectTestResourcesCleanDirectoryPath(
                "Grouped test result works"
            ),
            new SimpleTest<Path>(
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
        );
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

        final TestResult result = new GroupedTestResult<>(
            "Patched works",
            new PathPatched(
                new PathNotProjectRoot(workingDirectory),
                new PathPatchCreatingFile(fileName, content)
            ),

            new SimpleTest<>(
                "File exists",
                path -> {
                    return path.resolve(fileName).toFile().exists();
                }
            ),
            new SimpleTest<>(
                "File content as expected",
                path -> {
                    return FileUtils.readFileToString(
                        path.resolve(fileName).toFile()
                    )
                        .contains(content);
                }
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());
    }

    @Test
    public final void patchSequentalWorks() {
        final String fileName = "abc.txt";
        final String fileContent = "cba";
        final DescribedTestResult result = new DescribedTestResult(
            "Sequental patch works",
            new SimpleTestResult<>(
                new PathPatched(
                    new PathNotProjectRoot(
                        new ProjectTestResourcesCleanDirectoryPath(
                            "Sequental patch works"
                        )
                    ),
                    new PatchSequential<>(
                        new PathPatchCreatingFile(fileName, "abc"),
                        new PathPatchCreatingFile(fileName, fileContent)
                    )
                ),
                new SimpleTest<>(
                    "File content is as expected",
                    x -> FileUtils.readFileToString(x.resolve(fileName).toFile())
                    .equals(fileContent)

                )
            )
        );

        System.out.println(result.text());
        Assertions.assertTrue(result.value());

    }
    
    @Test
    public final void tempFileWorks() throws IOException {
        Path path = new CurrentWorkingDirectory();
        try (AutoDeletingTempFilePath tempFilePath = new AutoDeletingTempFilePath(new CurrentWorkingDirectory(), "some")) {
            path = tempFilePath;
            final String data = "123";
            FileUtils.writeStringToFile(tempFilePath.toFile(), data);
            Files.readAllLines(tempFilePath.toFile().toPath()).contains(data);
        }
        Assertions.assertFalse(path.toFile().exists());
    }
}
