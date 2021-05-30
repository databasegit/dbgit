package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.platform.commons.util.ExceptionUtils;
import ru.fusionsoft.dbgit.integration.primitives.SimpleTestResult;
import ru.fusionsoft.dbgit.integration.primitives.Test;
import ru.fusionsoft.dbgit.integration.primitives.TestResult;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfConsoleWhenRunning;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsWithMaskedCredentials;

public class ReportOfTestGroupRun<Subj> extends CharSequenceEnvelope {
    @SafeVarargs
    public ReportOfTestGroupRun(CharSequence description, Subj subject, Test<Subj>... tests) {
        super(() -> {
            return new CharsWithMaskedCredentials(new CharsOf<Subj>(() -> {
                try {
                    final String subjectConsoleOutput = String.valueOf(new CharsOfConsoleWhenRunning(subject::toString));
                    
                    final List<TestResult> testResults = Arrays
                        .stream(tests)
                        .map(x -> new SimpleTestResult<>(subject, x))
                        .collect(Collectors.toList());
                    
                    return new ReportOfTestFormat(
                        new LabelOfTestRunResult(testResults.stream().allMatch(TestResult::value)), 
                        description,
                        subjectConsoleOutput,
                        testResults.stream().map(TestResult::text).collect(Collectors.joining("\n"))
                    );
                } catch (CharsOfConsoleWhenRunning.CharsOfConsoleWhenRunningException e) {
                    return new ReportOfTestFormat(
                        new LabelOfTestRunBrokenSubject(), 
                        description, 
                        e.getMessage(),
                        ExceptionUtils.readStackTrace(e.getCause().getCause())
                    );
                } 

            }));
        });
    }
}
