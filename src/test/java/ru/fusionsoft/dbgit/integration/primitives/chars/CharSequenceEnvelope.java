package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.util.stream.IntStream;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;

public abstract class CharSequenceEnvelope implements CharSequence {
    private final SafeScalar<CharSequence> origin;

    private CharSequenceEnvelope(final SafeScalar<CharSequence> origin) {
        this.origin = origin;
    }

    public CharSequenceEnvelope(final Scalar<CharSequence> origin) {
        this(
            new SafeScalarOf<>(
                new StickyScalar<>(origin)
            )
        );
    }


    @Override
    public final int length() {
        return origin.value().length();
    }

    @Override
    public final char charAt(int i) {
        return origin.value().charAt(i);
    }

    @Override
    public final CharSequence subSequence(int i, int i1) {
        return origin.value().subSequence(i, i1);
    }

    @Override
    public final IntStream chars() {
        return origin.value().chars();
    }

    @Override
    public final IntStream codePoints() {
        return origin.value().codePoints();
    }

    @Override
    public final String toString() {
        return origin.value().toString();
    }
}
