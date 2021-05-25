package ru.fusionsoft.dbgit.integration.primitives;
import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsQuotedToRegexPattern;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.LabelOfTestRunResult;

public class DescribedTestResult<Subj> implements TestResult {
    private final String description;
    private final TestResult testResult;

    public DescribedTestResult(String description, TestResult testResult) {
        this.description = description;
        this.testResult = testResult;
    }

    public DescribedTestResult(String description, Subj subject, Test<Subj> test) {
        this(description, new SimpleTestResult<>(subject, test));
    }

    @Override
    public final boolean value() {
        return this.testResult.value();
    }

    @Override
    public final String text() {
        final CharSequence valueChars = new CharsQuotedToRegexPattern(
            new LabelOfTestRunResult(this.testResult.value())
        );
        
        return this.testResult
        .text()
        .replaceFirst(
            String.valueOf(valueChars), 
            MessageFormat.format(
                "{0} {1}",
                valueChars,
                this.description
            )
            
        );
      
    }

}
