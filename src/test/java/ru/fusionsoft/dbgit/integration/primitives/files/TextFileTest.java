package ru.fusionsoft.dbgit.integration.primitives.files;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class TextFileTest {

    private final Path path;

    public TextFileTest(Path path) {
        this.path = path;
    }

    public TextFileTest() {
        this(new CurrentWorkingDirectory());
    }

    @Test
    public void createsFile() throws IOException {
        String relativePath = ".dbgit/.dblink";

        Assertions.assertEquals(
            new FileContent(this.path, relativePath)
            .updateText("ABC")
            .text(),

            FileUtils.readFileToString(
                this.path.resolve(relativePath).toFile()
            )
        );
    }

    @Test
    public void updatesContent() throws IOException {
        File file = this.path.resolve("file").toFile();
        String content = "CBD";

        new FileContent(file).updateText(content);

        Assertions.assertEquals(
            content,
            FileUtils.readFileToString(file)
        );
    }
    
    @Test
    public void pathTest() {
        System.out.println();
    }
}
