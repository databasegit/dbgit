package ru.fusionsoft.dbgit.integration.primitives.chars;

import ru.fusionsoft.dbgit.integration.primitives.Test;

public class TestResultShortChars<Subject> extends CharsOf<Subject> {
    public TestResultShortChars(Subject subject, Test<Subject> test) {
        super(
            () -> {
                return String.valueOf(
                    new TestResultDetailedChars<>(subject, test)
                )
                .split("\n")[0];
            }
        );
    }
}
