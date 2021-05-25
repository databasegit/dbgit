package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

public class LabelOfTestRunResult extends LabelOfTestRun {
    public LabelOfTestRunResult(boolean checkResult) {
        super(checkResult ? "TEST OK" : "TEST FAIL");
    }
}
