package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.util.Arrays;
import java.util.List;

public class LinesOfString implements Lines {
    private final String string;

    public LinesOfString(String string) {
        this.string = string;
    }

    @Override
    public final List<String> list() {
        return Arrays.asList(string.split("\n"));
    }
}
