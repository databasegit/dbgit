package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfLines;
import ru.fusionsoft.dbgit.integration.primitives.files.AutoDeletingPath;
import ru.fusionsoft.dbgit.integration.primitives.files.TempFilePath;

public class PathPatchRunningProcessFrom implements Patch<Path> {

    private final Args processRunCommandLine;
    private final PrintStream printStream;

    public PathPatchRunningProcessFrom(Args processRunCommandLine, PrintStream printStream) {
        this.processRunCommandLine = processRunCommandLine;
        this.printStream = printStream;
    }

    @Override
    public final void apply(Path workingDirectory) throws Exception {
        final Consumer<CharSequence> outputConsumer = printStream::println;
        outputConsumer.accept(MessageFormat.format(
            "{0} # {1}",
            workingDirectory.toString(),
            String.join(" ", processRunCommandLine.values())
        ));
        
        try (
            final AutoDeletingPath tempOutPath = new AutoDeletingPath(new TempFilePath(workingDirectory.resolve("../"), "out"));
            final AutoDeletingPath tempErrPath = new AutoDeletingPath(new TempFilePath(workingDirectory.resolve("../"), "err"));
        ) {

            final Process process = new ProcessBuilder()
            .directory(workingDirectory.toFile())
            .command(
                Arrays.stream(processRunCommandLine.values())
                    .map(String::valueOf)
                    .collect(Collectors.toList())
            )
            .redirectOutput(tempOutPath.toFile())
            .redirectError(tempErrPath.toFile())
            .start();
            process.getOutputStream().close();
            
            final int exitCode = process.waitFor();
            outputConsumer.accept(new CharsOfLines(Files.readAllLines(tempOutPath.toFile().toPath()), "\n", "> "));
            
            if (exitCode != 0) {
                throw new Exception(MessageFormat.format(
                    "Process exited with error, code {0}\nErrors: {1}", 
                    exitCode, 
                    new CharsOf<>(()->{
                        final List<String> lines = Files.readAllLines(tempErrPath.toFile().toPath());
                        return lines.isEmpty()
                            ? "...error stream was empty"
                            : new CharsOfLines(lines, "", "\n> ");
                    })
                ));
            }
            
        }
    }
}
