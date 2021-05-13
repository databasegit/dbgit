package ru.fusionsoft.dbgit.integration.primitives;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.chars.TestSuccessMarkChars;

public class GrouppedTR<Subj> implements TestResult {
    private final Collection<TestResult> testResults;
    
    @SafeVarargs
    public GrouppedTR(Subj subject, Test<Subj>... tests ) {
        this.testResults = Arrays.stream(tests)
        .map(test -> new SimpleTestResult<>(subject, test))
        .collect(Collectors.toList());
    }

    @Override
    public final boolean value() {
        return testResults
        .stream()
        .allMatch(TestResult::value);
    }

    @Override
    public final String text() {
        return new TestSuccessMarkChars(this.value()) 
           + "\n\t"  
           + testResults
            .stream()
            .map(TestResult::text)
            .collect(Collectors.joining("\t"));
    }
}
