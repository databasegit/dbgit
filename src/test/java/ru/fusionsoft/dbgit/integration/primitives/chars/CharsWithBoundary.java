package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.text.MessageFormat;

public class CharsWithBoundary extends CharSequenceEnvelope {
    public CharsWithBoundary(CharSequence origin, CharSequence boundaryChars) {
        super(()-> MessageFormat.format("{1}{0}{1}", origin, boundaryChars));
    }
}
