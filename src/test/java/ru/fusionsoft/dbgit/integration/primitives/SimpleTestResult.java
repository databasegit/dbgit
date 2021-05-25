package ru.fusionsoft.dbgit.integration.primitives;

import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.LabelOfTestRunResult;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.ReportOfTestRun;

public class SimpleTestResult<Subj> implements TestResult {
    private final CharSequence text;
    public SimpleTestResult(Subj subject, Test<Subj> test) {
        this.text = new ReportOfTestRun<>(subject, test);
    }

    @Override
    public final String text() {
        return String.valueOf(this.text);
    }

    @Override
    public final boolean value() {
        return this
            .text()
            .contains(String.valueOf(new LabelOfTestRunResult(true)));
    }
}
