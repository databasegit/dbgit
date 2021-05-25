package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;
import java.util.List;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class CharsOfLines extends CharSequenceEnvelope {
    public CharsOfLines(Lines lines, CharSequence linePrefix) {
        super(() -> {
            return lines.list().stream().map(x -> linePrefix + x).collect(Collectors.joining("\n"));
        });
    }
    public CharsOfLines(List<String> list, CharSequence linePrefix) {
        super(() -> {
            return list.stream().map(x -> linePrefix + x).collect(Collectors.joining("\n"));
        });
    }

    public CharsOfLines(Lines lines) {
        this(lines, "");
    }
}
