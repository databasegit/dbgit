package ru.fusionsoft.dbgit.integration.primitives.path;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;


public class PathPrintsToConsole extends PathEnvelope {

    public PathPrintsToConsole(String text, Path origin) {
        super(() -> {
            System.out.println(text);
            return origin;
        });
    }
    public PathPrintsToConsole(Path origin) {
        this(
            "--> " + origin.toAbsolutePath() +
            Arrays
                .stream(origin.toFile().listFiles())
                .map( file -> file.getName() + (file.isDirectory() ? " (dir) " : "") )
                .collect(Collectors.joining("\n")),
            origin
        );
    }
}
