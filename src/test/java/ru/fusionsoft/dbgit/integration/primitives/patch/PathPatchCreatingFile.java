package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfPathWithComment;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class PathPatchCreatingFile extends PathPatchWithPrintStream {
    private final String name;
    private final String content;

    public PathPatchCreatingFile(final String name, final CharSequence content, PrintStream printStream) {
        super(printStream);
        this.name = name;
        this.content = String.valueOf(content);
    }

    public PathPatchCreatingFile(final String name, final CharSequence content) {
        this(name, content, new DefaultPrintStream());
    }

    @Override
    public final void apply(final Path root) throws Exception {
        this.printStream.println(
            new CharsOfPathWithComment(
                root,
                "writing file " + name + " with content:\n"
                + Arrays.stream(content.split("\n"))
                .map( x-> "> " + x)
                .collect(Collectors.joining(System.getProperty("line.separator")))
            )
        );
        final File file = root.resolve(this.name).toFile();
        file.getParentFile().mkdirs();
        FileUtils.writeStringToFile(file, this.content);
    }
}
