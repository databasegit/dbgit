package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import ru.fusionsoft.dbgit.integration.primitives.RunnableWithException;

public class SavedConsoleText {
    private final RunnableWithException runnable;
    
    public SavedConsoleText(RunnableWithException runnable) {
       this.runnable = runnable;
    }

    public final String text() throws Exception{
        final PrintStream original = System.out;
        try (final ByteArrayOutputStream cachedOutputStream = new ByteArrayOutputStream();) {
            try {

                System.setOut(new PrintStream(
                    cachedOutputStream,
                    true,
                    "UTF-8"
                ));
                
                runnable.run();
                return cachedOutputStream.toString();

            } catch (RuntimeException e) {
                final Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new RuntimeException(
                    "Runtime exception occurred while catching this console output:\n"
                    + cachedOutputStream.toString(), 
                    e
                );
            } catch (Exception e) {
                throw new Exception(
                    "Exception occurred while catching this console output:\n"
                    + cachedOutputStream.toString(), 
                    e
                );
            } catch (AssertionError e) {
                final Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new Error(
                    "Assertion failed while catching this console output:\n"
                    + cachedOutputStream.toString(),
                    e
                );
            } catch (Error e) {
                final Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new Error(
                    "Error occurred while catching this console output:\n"
                    + cachedOutputStream.toString(), 
                    e
                );
            } finally {
                System.setOut(original);
            }
        }
    }

    public final List<String> lines() throws Exception{
        return Arrays.asList(this.text().split("\\n"));
    }
    
}
