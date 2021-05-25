package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public abstract class LabelOfTestRun extends CharSequenceEnvelope {
    public LabelOfTestRun(CharSequence text) {
        super(()-> MessageFormat.format("[{0}]", text));
    }
}
