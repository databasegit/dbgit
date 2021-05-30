package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.StringTokenizer;
import ru.fusionsoft.dbgit.integration.primitives.RunnableWithException;

public class CharsOfConsoleWhenRunning extends CharsOf<PrintStream> {

    public CharsOfConsoleWhenRunning(RunnableWithException runnable) {
        super(() -> {
            final PrintStream original = System.out;
            try (final ByteArrayOutputStream cachedOutputStream = new ByteArrayOutputStream();) {
                try {
                    System.setOut(new PrintStream(cachedOutputStream, true, "UTF-8"));
                    
                    runnable.run();
                    return cachedOutputStream.toString();

                } catch (Throwable e) {
                    throw new CharsOfConsoleWhenRunningException(cachedOutputStream, e);
                } finally {
                    System.setOut(original);
                }
            }
        });
    }
    
    public static class CharsOfConsoleWhenRunningException extends Error {
        public CharsOfConsoleWhenRunningException(String text, Throwable cause) {
            super(
                (text.isEmpty() ? "Execution broken after:\n" : "") + text, 
                cause
            );
        }
        public CharsOfConsoleWhenRunningException(ByteArrayOutputStream byteArrayOutputStream, Throwable cause) {
            this(byteArrayOutputStream.toString(), cause);
        }
    }
}
