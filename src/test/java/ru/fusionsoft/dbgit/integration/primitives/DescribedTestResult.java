package ru.fusionsoft.dbgit.integration.primitives;
import java.text.MessageFormat;
import java.util.regex.Pattern;
import ru.fusionsoft.dbgit.integration.primitives.chars.TestSuccessMarkChars;

public class DescribedTestResult<Subj> implements TestResult {
    private final String description;
    private final TestResult testResult;

    public DescribedTestResult(String description, Subj subject, Test<Subj> test) {
        this.description = description;
        this.testResult = new SimpleTestResult<>(subject, test);
    }
    
    public DescribedTestResult(String description, TestResult simpleTestResult) {
        this.description = description;
        this.testResult = simpleTestResult;
    }

    @Override
    public final boolean value() {
        return this.testResult.value();
    }

    @Override
    public final String text() {
        final String valueChars = String.valueOf(
            new TestSuccessMarkChars(this.testResult.value())
        );
        
        return this.testResult
        .text()
        .replaceFirst(
            Pattern.quote(valueChars), 
            MessageFormat.format(
                "{0} {1} -",
                valueChars,
                this.description
            )
            
        );
      
    }

}
