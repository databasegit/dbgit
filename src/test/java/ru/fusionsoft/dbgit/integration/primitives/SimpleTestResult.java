package ru.fusionsoft.dbgit.integration.primitives;

import ru.fusionsoft.dbgit.integration.primitives.chars.TestSuccessMarkChars;
import ru.fusionsoft.dbgit.integration.primitives.chars.TestResultDetailedChars;

public class SimpleTestResult<Subj> implements TestResult {
    
    private final CharSequence text;
    
    public SimpleTestResult(Subj subject, Test<Subj> test) {
        this.text = new TestResultDetailedChars<>(subject, test);
    }    
    
    public SimpleTestResult(Subj subject, Function<Subj, Boolean> testFunction) {
        this.text = new TestResultDetailedChars<>(subject, new SimpleTest<>(testFunction));
    }

    @Override
    public final String text() {
        return String.valueOf(this.text);
    }

    @Override
    public final boolean value() {
        return this.text().contains(
            String.valueOf(new TestSuccessMarkChars(true))
        );
    }
}
