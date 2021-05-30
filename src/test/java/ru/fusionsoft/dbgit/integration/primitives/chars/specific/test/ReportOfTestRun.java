package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.platform.commons.util.ExceptionUtils;
import ru.fusionsoft.dbgit.integration.primitives.Test;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfConsoleWhenRunning;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsWithMaskedCredentials;

public class ReportOfTestRun<Subj> extends CharSequenceEnvelope {
    public ReportOfTestRun(Subj subject, Test<Subj> test) {
        super(() -> {
            return new CharsWithMaskedCredentials(new CharsOf<Subj>(() -> {
                
                try {
                    final CharSequence subjDetails = String.valueOf(new CharsOfConsoleWhenRunning(subject::toString));
                    try {
                        final AtomicBoolean value = new AtomicBoolean(false);
                        final CharSequence testDetails = String.valueOf(new CharsOfConsoleWhenRunning(()-> value.set(test.value(subject))));
                        final LabelOfTestRun label = new LabelOfTestRunResult(value.get());
                        return new ReportOfTestFormat(
                            label, 
                            test.description(), 
                            subjDetails, 
                            testDetails
                        );
                    } catch (Throwable testThrowable) {
                        return new ReportOfTestFormat(
                            new LabelOfTestRunExceptional(),
                            test.description(), 
                            ExceptionUtils.readStackTrace(testThrowable)
                        );
                    }
                } catch (CharsOfConsoleWhenRunning.CharsOfConsoleWhenRunningException e) {
                    return new ReportOfTestFormat(
                        new LabelOfTestRunBrokenSubject(), 
                        test.description(), 
                        e.getMessage(),
                        ExceptionUtils.readStackTrace(e.getCause().getCause())
                    );
                }
                
            }));
        });
    }
}
