package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.nio.file.Path;

public class CharsOfPathWithComment extends CharsOf<Path> {
    public CharsOfPathWithComment(Path origin, CharSequence comment) {
        super(()->{
            return origin.toAbsolutePath().toString() + " # " + comment;
        });
    }
}
