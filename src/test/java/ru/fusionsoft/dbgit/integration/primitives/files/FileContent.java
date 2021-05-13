package ru.fusionsoft.dbgit.integration.primitives.files;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class FileContent implements TextResource {
    private final File file;

    public FileContent(final File file) {
        this.file = file;
    }

    public FileContent(final Path path) {
        this(path.toFile());
    }

    public FileContent(final Path workingDirectory, final String... name) {
        this(workingDirectory.resolve(String.join("//", name)));
    }

    @Override
    public final String text() throws IOException {
        return FileUtils.readFileToString(this.file);
    }

    @Override
    public final TextResource updateText(final String content) throws IOException {
        this.file.getParentFile().mkdirs();
        FileUtils.writeStringToFile(this.file, content);
        return this;
    }

    @Override
    public final TextResource delete() throws IOException {
        if (this.file.exists()) {
            FileUtils.forceDelete(this.file);
        }
        return this;
    }
}
