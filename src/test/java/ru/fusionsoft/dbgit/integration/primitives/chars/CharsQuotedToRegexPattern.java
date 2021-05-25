package ru.fusionsoft.dbgit.integration.primitives.chars;

public class CharsQuotedToRegexPattern extends CharSequenceEnvelope {
    public CharsQuotedToRegexPattern(CharSequence origin) {
        super(() -> {
            return String.valueOf(origin).replaceAll("[\\W]", "\\\\$0");
        });
    }
}
