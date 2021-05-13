package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.platform.commons.util.ExceptionUtils;
import ru.fusionsoft.dbgit.integration.primitives.Test;

public class TestResultDetailedChars<Subject> extends CharsOf<Subject> {

    public TestResultDetailedChars(Subject subject, Test<Subject> test) {
        super(
            () -> {
                CharSequence details;
                CharSequence successMarkChars;
                
                try {
                    final AtomicBoolean value = new AtomicBoolean(false);
                    details = new SavedConsoleText(
                        () -> {
                            value.set(test.value(subject));
                        }
                    ).text();
                    successMarkChars = new TestSuccessMarkChars(value.get());
                    
                } catch (Error e) {
                    details = ExceptionUtils.readStackTrace(e.getCause());
                    successMarkChars = new TestSuccessMarkChars(e);
                } catch (RuntimeException e) {
                    details = ExceptionUtils.readStackTrace(e.getCause());
                    successMarkChars = new TestSuccessMarkChars(e);
                } catch (Exception e) {
                    details = ExceptionUtils.readStackTrace(e.getCause());
                    successMarkChars = new TestSuccessMarkChars(e);
                }
                return MessageFormat.format(
                    "{0} {1}\n{2}",
                    successMarkChars,
                    test.description(),
                    details
                );
            }
        );
    }
}
