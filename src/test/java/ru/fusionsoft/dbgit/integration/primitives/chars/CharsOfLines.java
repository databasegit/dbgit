package ru.fusionsoft.dbgit.integration.primitives.chars;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;

public class CharsOfLines extends CharSequenceEnvelope {
    public CharsOfLines(SafeScalar<List<? extends CharSequence>> charsListScalar, CharSequence delimiterChars, CharSequence itemPrefixChars) {
        super(() -> {
                return charsListScalar
                .value()
                .stream()
                .map(x -> String.valueOf(itemPrefixChars) + x)
                .collect(Collectors.joining(delimiterChars));
            }
        );
    }
    public CharsOfLines(Lines lines, CharSequence delimiterChars, CharSequence itemPrefixChars) {
        this( new SafeScalarOf<>(lines::list), delimiterChars, itemPrefixChars );
    }
    public CharsOfLines(List<? extends CharSequence> list, CharSequence delimiterChars, CharSequence itemPrefixChars) {
        this( new SafeScalarOf<>(()->list), delimiterChars, itemPrefixChars);
    }
    public CharsOfLines(Args args, CharSequence delimiterChars, CharSequence itemPrefixChars) {
        this( new SafeScalarOf<>(()-> Arrays.asList(args.values())), delimiterChars, itemPrefixChars);
    }
    public CharsOfLines(Args args) {
        this( args, "\n", "");
    }
    public CharsOfLines(Lines lines) {
        this(lines, "\n", "");
    }
    public CharsOfLines(List<? extends CharSequence> list) {
        this(list, "\n", "");
    }
}
