package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.chars.InputStreamChars;

public class PathPatchRunningProcessFrom implements Patch<Path> {

    private final Args processRunCommandLine;
    private final PrintStream printStream;

    public PathPatchRunningProcessFrom(Args processRunCommandLine, PrintStream printStream) {
        this.processRunCommandLine = processRunCommandLine;
        this.printStream = printStream;
    }

//    public PathPatchRunningProcessFrom(Args processRunCommandLine) {
//        this.processRunCommandLine = processRunCommandLine;
//        this.printStream = System.out;
//    }

    @Override
    public final void apply(Path root) throws Exception {
        try (
            final ByteArrayOutputStream cachedOutputStream = new ByteArrayOutputStream();
            final PrintStream cachedPrintStream = new PrintStream(
                cachedOutputStream,
                true,
                "UTF-8"
            )
        ) {
            final Consumer<CharSequence> outputConsumer = (chars) -> {
                cachedPrintStream.println(chars);
                printStream.println(chars);
            };
            
            outputConsumer.accept(MessageFormat.format(
                "{0} # {1}",
                root.toString(),
                String.join(" ", processRunCommandLine.values())
            ));

            final Process process = new ProcessBuilder()
            .directory(root.toAbsolutePath().toFile())
            .command(
                Arrays.stream(processRunCommandLine.values())
                .map(String::valueOf)
                .collect(Collectors.toList())
            )
            .start();

            final CharSequence processOutput = String.valueOf(new InputStreamChars(
                process.getInputStream()
            ));
            final CharSequence processErrOutput = new InputStreamChars(
                process.getErrorStream(), "Cp866"
            );

            final int exitCode = process.waitFor();
            process.destroyForcibly();
            outputConsumer.accept(processOutput);

            if (exitCode != 0) {
                throw new RuntimeException(MessageFormat.format(
                    "Process exited with error, code {0}" 
                    + "\nErrors: {1}" 
                    + "\nOriginal output: {2}"
                    ,exitCode
                    ,processErrOutput.length() != 0 
                        ? "\n" + processErrOutput 
                        : "...error stream was empty"
                    ,cachedOutputStream.size() != 0
                        ? "\n" + cachedOutputStream.toString()
                        : "...output stream was empty"
                ));
            }
        }
    }
}
