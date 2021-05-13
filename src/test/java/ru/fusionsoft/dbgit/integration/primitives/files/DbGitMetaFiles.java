package ru.fusionsoft.dbgit.integration.primitives.files;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbGitMetaFiles implements TextResourceGroup {
    private final Path workingDirectory;

    public DbGitMetaFiles(Path workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    @Override
    public final void add(String name, String content) throws IOException {
        new FileContent(this.workingDirectory, name).updateText(content);
    }

    @Override
    public final void clean() throws IOException {
        for (TextResource textFile : this.all().values()) {
            textFile.delete();
        }
    }

    @Override
    public final TextResource file(String... name){
        return new FileContent(this.workingDirectory, name);
}

    @Override
    public final Map<String, TextResource> all() throws IOException {
        try (Stream<Path> paths = Files.walk(this.workingDirectory.resolve(".dbgit"))) {
            return paths
                .filter( x->x.getParent().getParent().toFile().getName().equals(".dbgit") )
                .filter(Files::isRegularFile )
                .peek(System.out::println)
                .collect(Collectors.toMap( x->x.toFile().getName(), FileContent::new) );
        }
    }


}
