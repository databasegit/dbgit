package ru.fusionsoft.dbgit.integration.primitives;

import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.LabelOfTestRunResult;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.LinesOfString;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.ReportOfTestGroupRun;

public class GroupedTestResult<Subj> implements TestResult {
    private final CharSequence text;
    
    @SafeVarargs
    public GroupedTestResult(CharSequence description, Subj subject, Test<Subj>... tests ) {
        this.text = new ReportOfTestGroupRun<>(description, subject, tests);
    }

    @Override
    public final boolean value() {
        return new LinesOfString(
            String.valueOf(this.text)
        ).list().get(0)
        .contains(new LabelOfTestRunResult(true));
    }

    @Override
    public final String text() {
        return String.valueOf(this.text);
    }
}
